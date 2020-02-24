package focal_analysis

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector.io._
import geotrellis.vector.io.json._

import scala.io.StdIn.{readInt, readLine}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff._

import scala.io.Source
import geotrellis.vector._
import geotrellis.vector.io._
import spray.json._
import spray.json.DefaultJsonProtocol._
import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal._
//Square
import datasets.rasterdatasets.myRaster
//Has TileLayout Object, MultibandTile
import geotrellis.raster.io.geotiff._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.rasterize._
import geotrellis.raster.rasterize.polygon._
import geotrellis.raster.mapalgebra.local._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._

//Vector Json
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._

//ProjectedExtent object
import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD


import scala.io.StdIn
import java.io.File
import java.io._
//File Object


object Main {

  def countPixels(a:Int, b:geotrellis.raster.Tile) : Int = {
    /*
    Count the pixels

    Input:
      a = An integer to check against
      b = The tile to check

    Output:
      Sum of pixels
    */
    var pixelCount:Int = 0
    b.foreach {z => if(z==a) pixelCount += 1}
    pixelCount
  }

  def countPixelsSpark(a:Int, b:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey,
    geotrellis.raster.Tile)]) = {
    //The code below could potentially be simplified by using mapValues on the pair RDD vs map on the normal RDD.
    /*
    Count the pixels with spark

    Input:
      a = An integer to check against
      b = Resilient distributed dataset containing a spatial keys and data

    Output:
      (time taken, sum of pixels)
    */
    var countPixelStart = System.currentTimeMillis()
    val RDDValues: org.apache.spark.rdd.RDD[geotrellis.raster.Tile] = b.values
    val y = RDDValues.map(x => countPixels(a,x))
    val sumOfPixels = y.collect.sum
    var countPixelStop = System.currentTimeMillis()
    val theTime: Double = countPixelStop - countPixelStart
    (theTime, sumOfPixels)
  }

  def main(args: Array[String]): Unit = {
    /*
    Main entry point for focal analysis

    Input:
      args = None

    Output:
      None
    */

    val dataName = args(1)
    val dataFile = args(2)
    val dataPixelVal = args(3).toInt
    val dataNewPixel = args(4).toInt
    val outCSVPath = args(5)

    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic,dataset,tilesize,focalMeantime,counttime,type,run\n")

    val rasterDatasets = List(
      new myRaster(dataName, dataFile, dataPixelVal, dataNewPixel),
    )

    // TODO: need to get tile sizes from command line
    val tilesizes = Array(25, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000)

    val conf = new SparkConf().setMaster("local[12]").setAppName("Spark Tiler").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)

    for(r<-rasterDatasets){

      for (x <- 1 to 3){

        for (tilesize <- tilesizes) {

          val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(r.thePath,
            HadoopGeoTiffRDD.Options.DEFAULT)
          val pValue = r.pixelValue
          val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(tilesize))
          
          val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType,
            rasterMetaData.layout)
          rasterRDD.unpersist()
        
          var datasetName : String = r.name

          //Call Spark Function for focal analysis
          // 3x3 square neighborhood
          val focalNeighborhood = Square(1)

          //RDD with (spatialKey, focalMean Raster)
          val meanTile = tiledRaster.mapValues(x=> x.focalMean(focalNeighborhood))
          var (memoryTime, numMemoryPixels) = countPixelsSpark(pValue, meanTile)
          writer.write(s"pixlecount,$datasetName,$tilesize,$memoryTime,memory,$x\n")
          
          var (cachedTime, numCachedPixels) = countPixelsSpark(pValue, meanTile)
          writer.write(s"pixlecount,$datasetName,$tilesize,$cachedTime,cache,$x\n")

          tiledRaster.unpersist()
          meanTile.unpersist()

        }

      }

    }
    writer.close()
    sc.stop()
  }

}
