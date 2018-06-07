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
import org.apache.hadoop.fs.Path

//File Object
// extends java.io.Serializable
object raster{
  val the_path = "/home/david/Downloads/glc2000.tif"
  val the_name = "glc"
}



object CountGeoTiff{
  //Class for testing raster analytics
/*  def apply(name: String, thePath: String, pixelValue: Int, newPixel: Int) {
    val raster_name: String = name
    val raster_path: String = thePath
    var raster_pixel_value: Int = pixelValue
    var raster_new_pixel: Int = newPixel
    (name, thePath)
  }*/
  def info(name: String, thePath: String, pixelValue: Int, newPixel: Int): (String, String, Int, Int) = {
    println(s"INFO: $name" )
    (name, thePath, pixelValue, newPixel)
  }

  def AddData(name: String, thePath: String, pixelValue: Int, newPixel: Int) {
    var raster_name: String = name
    var raster_path: String = thePath
    var raster_pixel_value: Int = pixelValue
    var raster_new_pixel: Int = newPixel
    (name, raster_name)
  }



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

  /*def countRaster(theRaster:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)], oldValue:Int) : (Double, Int) = {

    var countPixelStart = System.currentTimeMillis()
    var foundPixels = countPixelsSpark(oldValue, theRaster)
    var countPixelStop = System.currentTimeMillis()
    val theTime = countPixelStop - countPixelStart
    //println(s"Milliseconds: ${countPixelStop - countPixelStart}")
    (theTime, foundPixels)
  }*/



  def reclassifyRaster(theRaster:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)], oldValue:Int, newValue:Int) : Double = {
    //Function for reclassifying raster

    //Type for evaluation statement
    type MyType = Int => Boolean
    var equalpixelvalue: MyType = (x: Int) => x == oldValue

    var reclassPixelStart = System.currentTimeMillis()
    var reclassedRaster = theRaster.localIf(equalpixelvalue, newValue, -999)
    var numReclassPixels = countPixelsSpark(newValue, reclassedRaster)
    var reclassPixelStop = System.currentTimeMillis()
    reclassedRaster.unpersist()
    val reclassTime = reclassPixelStop - reclassPixelStart
    reclassTime
  }

  //tiledRaster.

  /*  val rasterCount = tiledRaster.mapValues { tile =>
      tile.foreach { z =>
        if (z==12) PixelCount +=1
      }
    }*/

/*

  def countRaster(theRaster: RDD[(ProjectedExtent, Tile)], oldValue: Int) = {
    var countPixelStart = System.currentTimeMillis()
    var foundPixels = countPixelsSpark(oldValue, theRaster)
    var countPixelStop = System.currentTimeMillis()
    val theTime: Double = countPixelStop - countPixelStart
    //println(s"Milliseconds: ${countPixelStop - countPixelStart}")
    (theTime, foundPixels)
  }
*/

  def main(args: Array[String]): Unit = {
    //var raster = CountGeoTiff("glc", "/home/david/Downloads/glc2000.tif", 16, 1)
    val theRaster = new myRaster("glc", "/home/david/Downloads/glc2000.tif", 16, 1)
    //val theValues = CountGeoTiff.this.info("glc", "/home/david/Downloads/glc2000.tif", 16, 1)
    println(theRaster)
    //val raster_path = new Path(theRaster.thePath)




    //raster.AddData()
    //raster.AddData("glc", "/home/david/Downloads/glc2000.tif", 16, 1)

    /*val rasterDatasets = List(
      new rasterDataset("glc", "/home/david/Downloads/glc2000.tif", 16, 1)
      /*
      new rasterDataset("glc", "/data/projects/G-818404/glc2000_clipped.tif", 16, 1),
      new rasterDataset("meris", "/data/projects/G-818404/meris_2010_clipped.tif", 100, 1),
      new rasterDataset("nlcd", "/data/projects/G-818404/nlcd_2006.tif", 21, 1)
      new rasterDataset("meris_3m", "/data/projects/G-818404/meris_2010_clipped_3m.tif", 100, 1)
      */
    )*/

    val outCSVPath = "/home/david/test.csv" //"/home/04489/dhaynes/geotrellis_all_4_12_2018_12instances.csv"
    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic,dataset,tilesize,time,run\n")
    //Call Reclass Pixel Function (add one to specified value)
    //reclassPixels(pixel,rasterArray);

    val tilesizes = Array(25, 50, 100) //, 200, 300, 400, 500, 600, 700, 800, 900, 1000) //, 1500, 2000, 2500, 3000, 3500, 4000)
    //Using Spark

    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Tiler").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator").set("spark.driver.memory", "2g").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    //val geoTiffRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(theRaster.thePath, HadoopGeoTiffRDD.Options.DEFAULT)
      //val rasterRDD: RDD[(ProjectedExtent, geotrellis.raster.Tile)] = sc.hadoopGeoTiffRDD(raster_path)
/*      val geoTiff: SinglebandGeoTiff = GeoTiffReader.readSingleband(theValues._2, decompress = false, streaming = true)

      //val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(r.thePath, decompress = false, streaming = true)

      val pValue = theValues._3

      val ld = LayoutDefinition(geoTiff.rasterExtent, 50)
      val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(geoTiff.cellType, ld)

      val (analyticTime: Double, numPixels: Int) = countPixelsSpark(pValue, tiledRaster)*/
      // println(analyticTime)

    /*for (x <- 1 to 1){

      for (tilesize <- tilesizes) {
        val ld = LayoutDefinition(geoTiff.rasterExtent, tilesize)
        val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(geoTiff.cellType, ld)
        var datasetName : String = r.name

        //Call Spark Function to count pixels

        //analyticTime = countRaster(tiledRaster, pValue)
        //writer.write(s"pixlecount,$datasetName,$tilesize,$analyticTime,$x\n")

        //analyticTime = reclassifyRaster(tiledRaster, r.pixelValue, r.newPixel)
        //writer.write(s"reclassify,$datasetName,$tilesize,$analyticTime,$x\n")

        tiledRaster.unpersist()
      }

    }*/

    writer.close()
    sc.stop()
  }

}








