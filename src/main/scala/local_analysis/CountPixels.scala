package local_analysis

import org.apache.log4j.{Level, Logger}
import geotrellis.raster._
import geotrellis.spark.TileLayerMetadata
import scala.io.StdIn.{readLine,readInt}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
//Has TileLayout Object, MultibandTile
import geotrellis.raster.io.geotiff._
import scala.io.Source
import geotrellis.vector._
import geotrellis.vector.io._

import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._

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
import datasets.rasterdatasets.myRaster
import org.apache.hadoop.fs.Path


object CountPixels{
  //Class for testing raster local operation pixel count


  def countPixels(a:Int, b:geotrellis.raster.Tile) : Int = {
    /*
    Count the pixels

    Input:
      a = An integer to check against
      b = The tile to check against

    Output:
      Sum of pixels
    */
    var pixelCount:Int = 0
    b.foreach {z => if(z==a) pixelCount += 1}
    pixelCount
  }

  def countPixelsSpark(a:Int, b:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey,
    geotrellis.raster.Tile)]) = {
    /*
    Count the pixels with spark

    Input:
      a = An integer to check against
      b = Resilient distributed dataset containing a spatial keys and tiles, a raster

    Output:
      (time taken, sum of pixels)
    */
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
    /*
    Main entry point for pixel count class

    Input:
      args = None

    Output:
      None
    */
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val rasterDatasets = List(
      // TODO: need to take data externally somehow
      new myRaster("glc", "/home/david/Downloads/glc2000.tif", 16, 1)
      )
    // TODO: need to anonimize out path
    val outCSVPath = "/home/david/output.csv"  //"/data/projects/G-818404/geotrellis_localcount_8_27_2018_12instances.csv" //
    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic,dataset,tilesize,time,type,run\n")

    val tilesizes = Array(25) //, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000) //, 1500, 2000, 2500, 3000, 3500, 4000)

    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Tiler").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)

    for(r<-rasterDatasets){

      for (x <- 1 to 3){

        for (tilesize <- tilesizes) {

          val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(r.thePath),
            HadoopGeoTiffRDD.Options.DEFAULT)
          val pValue = r.pixelValue

          //Spark anonymous _
          val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(tilesize))

          val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType,
            rasterMetaData.layout)

          // remove original RDD with variable tiling from memory
          rasterRDD.unpersist()
          var datasetName : String = r.name

          //Call Spark Function to count pixels
          var (memoryTime, numMemoryPixels) = countPixelsSpark(pValue, tiledRaster)
          writer.write(s"pixlecount,$datasetName,$tilesize,$memoryTime,memory,$x\n")

          //Call Spark Function again to get the cached time
          var (cachedTime, numCachedPixels) = countPixelsSpark(pValue, tiledRaster)
          writer.write(s"pixlecount,$datasetName,$tilesize,$cachedTime,cache,$x\n")
   
          //Unpersist tiled raster
          tiledRaster.unpersist()
        }

      }

    }

    writer.close()
    sc.stop()
  }

}
