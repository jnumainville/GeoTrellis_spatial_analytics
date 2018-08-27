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

//Libraries for reading a json
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.io.StdIn
import java.io.File
import java.io._
import local_analysis.rasterdatasets.myRaster
import org.apache.log4j.{Level, Logger}


object ReclassifyTiff{
  //Class for testing raster local operation pixel count

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


  def reclassifyRaster(theRaster:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)], oldValue:Int, newValue:Int)  = {
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

    val rasterDatasets = List(
     //new myRaster("glc", "/home/david/Downloads/glc2000.tif", 16, 1)
      new myRaster("glc", "/data/projects/G-818404/glc2000_clipped.tif", 16, 83),
      new myRaster("meris", "/data/projects/G-818404/meris_2010_clipped.tif", 100, 83),
      new myRaster("nlcd", "/data/projects/G-818404/nlcd_2006.tif", 21, 83)
      // new myRaster("meris_3m", "/data/projects/G-818404/meris_3m/", 100, 83)
    )

    val outCSVPath = "/data/projects/G-818404/geotrellis_reclass_8_27_2018_12instances.csv"
    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic,dataset,tilesize,reclasstime,counttime,type,run\n")

    
    val tilesizes = Array(25, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000) //, 1500, 2000, 2500, 3000, 3500, 4000)

    val conf = new SparkConf().setMaster("local[12]").setAppName("Spark Tiler").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")//.set("spark.driver.memory", "2g").set("spark.executor.memory", "1g")
    implicit val sc = new SparkContext(conf)

    for(r<-rasterDatasets){

      for (x <- 1 to 3){

        for (tilesize <- tilesizes) {

          val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(r.thePath, HadoopGeoTiffRDD.Options.DEFAULT)
          val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(tilesize))
          
          val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout)
          rasterRDD.unpersist()
          
          var datasetName : String = r.name
          var pValue = r.pixelValue

          //Call Spark Function to Reclassify Raster
          var (reclassMemoryTime,reclassMemoryCountTime,reclassedTileRaster)  = reclassifyRaster(tiledRaster, r.pixelValue, r.newPixel)
          //println(reclassMemoryTime, reclassMemoryCountTime)
          writer.write(s"reclassify,$datasetName,$tilesize,$reclassMemoryTime,$reclassMemoryCountTime,memory,$x\n")

          //Reclassify again with pixel values switched
          var (reclassCachedTime,reclassCachedCountTime,reclassedCachedTileRaster)  = reclassifyRaster(reclassedTileRaster, r.newPixel, r.pixelValue)
          writer.write(s"reclassify,$datasetName,$tilesize,$reclassCachedTime,$reclassCachedCountTime,cached,$x\n")
          tiledRaster.unpersist()
        }

      }

    }

    writer.close()
    sc.stop()
  }

}
