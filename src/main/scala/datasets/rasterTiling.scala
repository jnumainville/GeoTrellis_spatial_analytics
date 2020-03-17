package datasets

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
import collection.JavaConversions._
import com.typesafe.config.{Config, ConfigFactory}



object rasterTiling {

  def main(args: Array[String]): Unit = {
    /*
    Entry point for raster tiling

    Input:
      args = In order:
        dataName = name of the raster
        dataFile = name of file where the raster is
        dataPixelVal = the current pixel value
        dataNewPixel = the new pixel value
        outputFolder = the folder to store the output in

    Output:
      None
    */

    val dataName = args(1)
    val dataFile = args(2)
    val dataPixelVal = args(3).toInt
    val dataNewPixel = args(4).toInt
    val outputFolder= args(5)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val rasterDatasets = List(
      new myRaster(dataName, dataFile, dataPixelVal, dataNewPixel)
    )

    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Tiler").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)

    val r = rasterDatasets(0)

    val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(r.thePath),
      HadoopGeoTiffRDD.Options.DEFAULT)

    //Tile Layout
    val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD,FloatingLayoutScheme(tileSize = 300))
    val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType,
      rasterMetaData.layout)

    //tiledRaster has the properties .dimensions and .size (no. of pixels) which might be useful.
    var dimensionsRDD = tiledRaster.mapValues(x=>x.dimensions)
    var sizeRDD = tiledRaster.mapValues(x=>x.size)
    sizeRDD.map(x => x._1.extent(rasterMetaData.layout).toString() + "," + x._2.toString()).
      saveAsTextFile(s"$outputFolder/${dataName}_layouttilesize")
    println(tiledRaster.count())

    val rasterRDD2: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(r.thePath),
      HadoopGeoTiffRDD.Options(chunkSize= Some(300)) )
    val dimensionsRDD2 = rasterRDD2.mapValues(x => x.dimensions)
    dimensionsRDD2.mapValues(x => x._1 + "," + x._2.toString()).saveAsTextFile(s"$outputFolder/${dataName}_chunkSize")
    rasterRDD2.mapValues(x => x.size.toString()).saveAsTextFile(path=s"$outputFolder/${dataName}_chunksize_size")
    println(rasterRDD2.count())

    val rasterRDD3: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(r.thePath),
      HadoopGeoTiffRDD.Options(maxTileSize = Some(300)) )
    val dimensionsRDD3 = rasterRDD3.mapValues(x => x.dimensions)
    dimensionsRDD3.mapValues(x => x._1 + "," + x._2.toString()).saveAsTextFile(s"$outputFolder/${dataName}_maxTileSize")
    rasterRDD3.mapValues(x => x.size.toString()).saveAsTextFile(path=s"$outputFolder/${dataName}_maxTileSize_size")
    println(rasterRDD3.count())

    sc.stop()
  }
}
