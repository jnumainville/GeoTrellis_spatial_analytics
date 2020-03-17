package local_analysis

import datasets.rasterdatasets.myRaster
import geotrellis.raster.Tile
import geotrellis.spark.tiling._
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.vector.ProjectedExtent
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import java.io._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import org.apache.spark.HashPartitioner
import org.apache.hadoop.fs.Path

// config
import collection.JavaConversions._
import com.typesafe.config.{Config, ConfigFactory}

object two_raster_add {


  def countPixels(a: Int, b: geotrellis.raster.Tile): Int = {
    /*
    Count the pixels

    Input:
      a = An integer to check against
      b = The tile to check against

    Output:
      Sum of pixels
    */
    var pixelCount: Int = 0
    b.foreach { z => if (z == a) pixelCount += 1 }
    pixelCount
  }

  def countPixelsSpark(a: Int, b: org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey,
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
    val y = RDDValues.map(x => countPixels(a, x))
    val sumOfPixels = y.collect.sum
    var countPixelStop = System.currentTimeMillis()
    val theTime: Double = countPixelStop - countPixelStart
    (theTime, sumOfPixels)
  }

  def twoRasterAdd(r1: org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)],
                   r2: org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)],
                   pixelValue: Int) = {
    /*
    Add two rasters

    Input:
      r1 = A resilient distributed dataset, the first raster to add
      r2 = A resilient distributed dataset, the second raster to add
      pixelValue = The pixel value to check against

    Output:
      (time taken to add, resulting raster)
    */
    var rasterAddStart = System.currentTimeMillis()

    /*Note from geotrellis documentation, if tiles are not aligned,
     result will only contain intersecting tiles.
     This is lazy evaluated*/
    val outputRaster = r1 + r2

    //expanding this to n rasters, can be done with List(a, b, c).localAdd

    var (countTime, numPixels) = countPixelsSpark(pixelValue, outputRaster)
    println(s"Found $numPixels pixels for value: $pixelValue ")
    var rasterAddStop = System.currentTimeMillis()
    val addingTime: Double = rasterAddStop - rasterAddStart
    //Could return countTime, too if that calculation is being done.
    (addingTime, outputRaster)
  }


  def main(args: Array[String]): Unit = {
    /*
    Main entry point for the two raster add function

    Input:
      args = In order:
        dataName = name of the raster
        dataFile = name of file where the raster is
        dataPixelVal = the current pixel value
        dataNewPixel = the new pixel value
        outCSVPath = where to output the CSV

    Output:
      None
    */

    val config: Config = ConfigFactory.load("datasets.conf")

    val dataName = args(1)
    val dataFile = args(2)
    val dataPixelVal = args(3).toInt
    val dataNewPixel = args(4).toInt
    val tileSizes = Array(25)
    val outCSVPath = config.getString("two_raster_add.outCSVPath")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //Raster Dataset Path
    val rasterDatasets = for {
      n <- dataName
      f <- dataFile
      v <- dataPixelVal
      p <- dataNewPixel
    } yield myRaster(n, f, v, p)


    //val writer = new BufferedWriter(new )
    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic,raster_dataset,tilesize,total_time,run\n")

    val conf = new SparkConf().setMaster("local[2]").setAppName("Zonal Stats").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    implicit val sc = new SparkContext(conf)
    for (x <- 1 to 3){

      for (r <- rasterDatasets) {
        val pValue = r.pixelValue * 2
        var datasetName : String = r.name

        for (tilesize <- tileSizes) {
          //val tilesize = 250

          //val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(r.thePath)
          val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(r.thePath),
            HadoopGeoTiffRDD.Options.DEFAULT)
          val (_, rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(tilesize))
          val raster1: RDD[(SpatialKey, geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType,
            rasterMetaData.layout)
          val raster2: RDD[(SpatialKey, geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType,
            rasterMetaData.layout)

          var (memoryTime, calcRaster) = twoRasterAdd(raster1, raster2, pValue)
          println(memoryTime)

          writer.write(s"two_raster_add,$datasetName,$tilesize,$memoryTime,memory,$x\n")

          var (cachedTime, calcCachedRaster) = twoRasterAdd(raster1, raster2, pValue)
          println(cachedTime)
          writer.write(s"two_raster_add,$datasetName,$tilesize,$cachedTime,cached,$x\n")
          raster1.unpersist()
          raster2.unpersist()

        }
      }
    }
    writer.close()
    sc.stop()
  }//main
}