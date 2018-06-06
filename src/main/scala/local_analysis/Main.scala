package local_analysis

import geotrellis.raster._
import geotrellis.spark._
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

//hadoop config libraries
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

//Libraries for reading a json
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.io.StdIn
import java.io.File
import java.io._
//File Object

object RDDCount {
  //Class for testing raster analytics

  def countPixels(a:Int, b:geotrellis.raster.Tile) : Int = {
    var pixelCount:Int = 0
    b.foreach {z => if(z==a) pixelCount += 1}
    pixelCount
  }

  def countPixelsSpark(a:Int, b:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)]) : Int = {
    //The code below could potentially be simplified by using mapValues on the pair RDD vs map on the normal RDD.
    val RDDValues: org.apache.spark.rdd.RDD[geotrellis.raster.Tile] = b.values
    val y = RDDValues.map(x => countPixels(a,x))
    val sumOfPixels = y.collect.sum
    sumOfPixels
  }

  def countRaster(theRaster:org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)], oldValue:Int) : Double = {
    
    var countPixelStart = System.currentTimeMillis()
    var thePixels = countPixelsSpark(oldValue, theRaster)
    var countPixelStop = System.currentTimeMillis()
    val theTime = countPixelStop - countPixelStart
    println(s"Milliseconds: ${countPixelStop - countPixelStart}")
    thePixels
    }  

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



  def main(args: Array[String]): Unit = {
    //Call Reclass Pixel Function (add one to specified value)
    //reclassPixels(pixel,rasterArray);


    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Geospatial Analysis").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")//.set("spark.driver.memory", "2g").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    try {
      // val inputRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(inputPath)
      val inputPath: String = "/home/david/Downloads/glc2000.tif"
      //val inputRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(inputPath)
      // class rasterDataset(val name: String, val thePath: String, var pixelValue: Int, var newPixel: Int)
      val tilesizes = Array(25, 50, 100) //, 200, 300, 400, 500, 600, 700, 800, 900, 1000) //, 1500, 2000, 2500, 3000, 3500, 4000)

      val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(inputPath, decompress = false, streaming = true)
      val rasterArray: geotrellis.raster.Tile = geoTiff.tile
      for (tilesize <- tilesizes) {
        val ld = LayoutDefinition(geoTiff.rasterExtent, tilesize)
        //val tiledRaster: RDD[(SpatialKey, geotrellis.raster.Tile)] = rasterArray.tileToLayout(geoTiff.cellType, ld)
        var datasetName: String = "glc2000"

        //Call Spark Function to count pixels
        //val numPixels = countRaster(tiledRaster, 16)
        println(tilesize)
        // writer.write(s"pixlecount,$datasetName,$tilesize,$numPixels,analyticTime,x\n")
      }
      //val aPath = "file://" + new File("/home/david/Downloads/glc2000.tif").getAbsolutePath
      // val rasterRDD: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("/home/david/Downloads/glc2000.tif")
      //run(sc)

      //val rasterRDD = sc.hadoopGeoTiffRDD("/home/david/SAGE",Seq("tif"))
    } finally {
      sc.stop()
    }
  }


    def run(implicit sc: SparkContext) = {
      // Read the geotiff in as a single image RDD,
      // using a method implicitly added to SparkContext by
      // an implicit class available via the
      // "import geotrellis.spark.io.hadoop._ " statement.


      val myreader = geotrellis.spark.io.hadoop.HadoopGeoTiffReader
      val rasterRDD = myreader.readSingleband("/home/david/Downloads/glc2000.tif")

      // val outCSVPath = "/home/david/Downloads/test.csv" //"/home/04489/dhaynes/geotrellis_all_4_12_2018_12instances.csv"
      // val writer = new PrintWriter("file://" + new File(outCSVPath))
      // writer.write("analytic,dataset,tilesize,time,run\n")

      val inputPath = "/home/david/Downloads/glc2000.tif"
      //val rasterRDD = sc.hadoopGeoTiffRDD(inputPath)
      //val inputPath = java.io.Serializable(r.thePath).getAbsolutePath
      val tilesizes = Array(25, 50, 100) //, 200, 300, 400, 500, 600, 700, 800, 900, 1000) //, 1500, 2000, 2500, 3000, 3500, 4000)
      val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(inputPath, decompress = false, streaming = true)
      val rasterArray: geotrellis.raster.Tile = geoTiff.tile

      for (tilesize <- tilesizes) {
        val ld = LayoutDefinition(geoTiff.rasterExtent, tilesize)
        //val tiledRaster: RDD[(SpatialKey, geotrellis.raster.Tile)] = rasterRDD.tileToLayout(geoTiff.cellType, ld)
        var datasetName: String = "glc2000"

        //Call Spark Function to count pixels
        //val numPixels = countRaster(tiledRaster, 16)
        println(tilesize)
        // writer.write(s"pixlecount,$datasetName,$tilesize,$numPixels,analyticTime,x\n")
      }

      // writer.close()

    }
  //val geoTiffRDD = HadoopGeoTiffRDD.spatial(new Path(localGeoTiffPath))

}
