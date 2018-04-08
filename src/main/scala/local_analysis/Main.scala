package local_analysis

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import scala.io.StdIn.{readLine,readInt}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff._
import scala.io.Source
import geotrellis.vector._
import geotrellis.vector.io._

//Has TileLayout Object, MultibandTile
import geotrellis.raster.io.geotiff._
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
//File Object

object RasterAnalytics {
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





  def main(args: Array[String]): Unit = {

    class rasterDataset(val name: String, val thePath: String, var pixelValue: Int, var newPixel: Int)

    val rasterDatasets = List(
      new rasterDataset("glc", "/data/projects/G-818404/glc2000_clipped.tif", 16, 1),
      new rasterDataset("meris", "/data/projects/G-818404/meris_2010_clipped.tif", 100, 1)
      // new rasterDataset("nlcd", "/data/projects/G-818404/nlcd_2006.tif", 21, 1)
    )

    val outCSVPath = "/home/04489/dhaynes/geotrellis_reclassify_4_6_2018_12instances.csv"
    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic, dataset, tilesize, pixlecount, time, run\n")
    //Call Reclass Pixel Function (add one to specified value)
    //reclassPixels(pixel,rasterArray);
    type MyType = Int => Boolean
    val tilesizes = Array(50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000, 2500, 3000, 3500, 4000)
    //Using Spark

    val conf = new SparkConf().setMaster("local[12]").setAppName("Spark Tiler").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    val sc = new SparkContext(conf)
    for(r <- rasterDatasets) {
      val rasterRDD: RDD[(ProjectedExtent, geotrellis.raster.Tile)] = sc.hadoopGeoTiffRDD(r.thePath)
      val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(r.thePath)
      val rasterArray: geotrellis.raster.Tile = geoTiff.tile

      for (x <- 1 to 3){

        for (tilesize <- tilesizes) {
          val ld = LayoutDefinition(geoTiff.rasterExtent, tilesize)
          val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(geoTiff.cellType, ld)

          //Call Spark Function to count pixels
          var countPixelStart = System.currentTimeMillis
          var numPixels = countPixelsSpark(r.pixelValue, tiledRaster)
          // println( "Using Spark: found pixleValue " + r.pixelValue + " occurences: " + countPixelsSpark(r.pixelValue, tiledRaster) )
          var countPixelStop = System.currentTimeMillis
          val analyticTime = countPixelStop - countPixelStart
          //println(s"Milliseconds: ${countPixelStop - countPixelStart}")
          var datasetName : String = r.name

          //writer.write(s"pixlecount, $datasetName, $tilesize, $numPixels, $analyticTime, $x\n")

          var reclassPixelStart = System.currentTimeMillis
          val theReclassValue = r.newPixel
          var equalpixelvalue: MyType = (x: Int) => x == theReclassValue
          var reclassedRaster = tiledRaster.localIf(equalpixelvalue, r.newPixel, -999)
          //var numReclassPixels = countPixelsSpark(r.newPixel, reclassedRaster)
          var reclassPixelStop = System.currentTimeMillis
          val reclassTime = reclassPixelStop - reclassPixelStart

          writer.write(s"reclassify, $datasetName, $tilesize, 0, $reclassTime, $x\n")
          tiledRaster.unpersist()
        }

      }

    }
    writer.close()
    sc.stop()
  }

}








