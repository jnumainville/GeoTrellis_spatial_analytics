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
import local_analysis.rasterdatasets.myRaster
//Has TileLayout Object, MultibandTile
import geotrellis.raster.io.geotiff._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.rasterize._
import geotrellis.raster.rasterize.polygon._
import geotrellis.raster.mapalgebra.local._
// import geotrellis.proj4._

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
    var pixelCount:Int = 0
    b.foreach {z => if(z==a) pixelCount += 1}
    pixelCount
  }

  def countPixelsSpark(a:Int, b:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)]) = {
    //The code below could potentially be simplified by using mapValues on the pair RDD vs map on the normal RDD.
    var countPixelStart = System.currentTimeMillis()
    val RDDValues: org.apache.spark.rdd.RDD[geotrellis.raster.Tile] = b.values
    val y = RDDValues.map(x => countPixels(a,x))
    val sumOfPixels = y.collect.sum
    var countPixelStop = System.currentTimeMillis()
    val theTime: Double = countPixelStop - countPixelStart
    (theTime, sumOfPixels)
  }

  def main(args: Array[String]): Unit = {

    val outCSVPath = "/data/projects/G-818404/geotrellis_focalcount_8_27_2018_12instances.csv"
    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic,dataset,tilesize,focalMeantime,counttime,type,run\n")

    val rasterDatasets = List(
      //new myRaster("glc", "/home/david/Downloads/glc2000.tif", 16, 1)
      new myRaster("glc", "/data/projects/G-818404/glc2000_clipped.tif", 16, 1),
      new myRaster("meris", "/data/projects/G-818404/meris_2010_clipped.tif", 100, 1),
      new myRaster("nlcd", "/data/projects/G-818404/nlcd_2006.tif", 21, 1)
      //new rasterdataset("meris_3m", "/data/projects/G-818404/meris_2010_clipped_3m/", 100, 1)
    )

    val tilesizes = Array(25, 50, 100) //, 200, 300, 400, 500, 600, 700, 800, 900, 1000) //, 1500, 2000, 2500, 3000, 3500, 4000)

    val conf = new SparkConf().setMaster("local[12]").setAppName("Spark Tiler").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")//.set("spark.driver.memory", "2g").set("spark.executor.memory", "1g")
    implicit val sc = new SparkContext(conf)

    for(r<-rasterDatasets){

      for (x <- 1 to 1){

        for (tilesize <- tilesizes) {

          val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(r.thePath, HadoopGeoTiffRDD.Options.DEFAULT)
          val pValue = r.pixelValue
          val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(tilesize))
          
          val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout)
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

          //Get sample values for the upper left corner of each spatial key
          //val sampleCorner = meanTile.mapValues(x=> x.getDouble(0, 0))

          //print the first five for confirmation
          //sampleCorner.take(5)
          tiledRaster.unpersist()
          meanTile.unpersist()

        }

      }

    }
    writer.close()
    sc.stop()
  }

}
