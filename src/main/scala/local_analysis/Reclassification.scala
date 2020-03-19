package local_analysis

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

//Libraries for reading a json
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.io.StdIn
import java.io.File
import java.io._
import datasets.rasterdatasets.myRaster
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.fs.Path

// config
import collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}


object Reclassification{
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

  def countPixelsSpark(a:Int, b:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)]) = {
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


  def reclassifyRaster(theRaster:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)],
                       oldValue:Int, newValue:Int)  = {
    /*
    Reclassify the raster

    Input:
      theRaster = A resilient distributed dataset, the raster to reclassify
      oldValue = An integer, the old value of the raster
      newValue = An integer, the new value of the raster

    Output:
      (time taken to reclassify, time taken to count, raster after it was reclassed)
    */
    //Function for reclassifying raster

    //Type for evaluation statement
    type ReclassType = Int => Boolean
    var equalpixelvalue: ReclassType = (x: Int) => x == oldValue

    var reclassPixelStart = System.currentTimeMillis()
    var reclassedRaster = theRaster.localIf(equalpixelvalue, newValue, -999)
    var (countTime, numPixels) = countPixelsSpark(newValue, reclassedRaster)
    var reclassPixelStop = System.currentTimeMillis()
    //reclassedRaster.unpersist()
    var reclassTime = reclassPixelStop - reclassPixelStart
    (reclassTime, countTime, reclassedRaster)
  }


  def main(args: Array[String]): Unit = {
    /*
    Entry point for reclassification

    Input:
      None

    Output:
      None
    */

    val config: Config = ConfigFactory.load("datasets.conf")

    val dataName = config.getStringList("Reclassification.dataName").asScala.toList
    val dataFile = config.getStringList("Reclassification.dataFile").asScala.toList
    val dataPixelVal = config.getIntList("Reclassification.dataPixelVal").asScala.toList
    val dataNewPixel = config.getIntList("Reclassification.dataNewPixel").asScala.toList
    val tilesizes = config.getIntList("Reclassification.tilesizes").asScala.toList
    val outCSVPath = config.getString("Reclassification.outCSVPath")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val rasterDatasets = for {
      n <- dataName
      f <- dataFile
      v <- dataPixelVal
      p <- dataNewPixel
    } yield myRaster(n, f, v, p)

    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic,dataset,tilesize,reclasstime,counttime,type,run\n")

    val conf = new SparkConf().setMaster("local[12]").setAppName("Spark Tiler").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)

    for(r<-rasterDatasets){

      for (x <- 1 to 1){

        for (tilesize <- tilesizes) {

          val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(r.thePath),
            HadoopGeoTiffRDD.Options.DEFAULT)
          val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(tilesize))
          
          val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType,
            rasterMetaData.layout)
          rasterRDD.unpersist()
          
          var datasetName : String = r.name
          var pValue = r.pixelValue

          //Call Spark Function to Reclassify Raster
          var (reclassMemoryTime,reclassMemoryCountTime,reclassedTileRaster)  = reclassifyRaster(tiledRaster,
            r.pixelValue, r.newPixel)
          //println(reclassMemoryTime, reclassMemoryCountTime)
          writer.write(s"reclassify,$datasetName,$tilesize,$reclassMemoryTime,$reclassMemoryCountTime,memory,$x\n")

          //Reclassify again with pixel values switched
          var (reclassCachedTime,reclassCachedCountTime,reclassedCachedTileRaster)  =
            reclassifyRaster(reclassedTileRaster, r.newPixel, r.pixelValue)
          writer.write(s"reclassify,$datasetName,$tilesize,$reclassCachedTime,$reclassCachedCountTime,cached,$x\n")
          tiledRaster.unpersist()
        }

      }

    }

    writer.close()
    sc.stop()
  }

}
