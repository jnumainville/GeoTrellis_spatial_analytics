package zonal_analysis

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


import spray.json._
import spray.json.DefaultJsonProtocol._


import geotrellis.raster._
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
//File Object


object Main {

  def main(args: Array[String]): Unit = {

    //Raster Layer
    val path: String = "/home/david/SAGE/mn_races.tif"

    val conf = new SparkConf().setMaster("local[12]").setAppName("Spark Tiler").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)

    val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(path)
    val rasterRDD: RDD[(ProjectedExtent, geotrellis.raster.Tile)] = sc.hadoopGeoTiffRDD(path)
    val ld = LayoutDefinition(geoTiff.rasterExtent, 100)
    val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(geoTiff.cellType, ld)

    //Vector Census Layer
    val file: String = "data/censusMetroNew.geojson"
    val census = scala.io.Source.fromFile(file).getLines.mkString
    case class SomeProp(NAME: String,LSAD: String,AFFGEOID: String,ALAND: Int,AWATER: Int)
    implicit val boxedToRead = jsonFormat5(SomeProp)

    val metroAll: Map[String, MultiPolygonFeature[SomeProp]] = census.parseGeoJson[JsonFeatureCollectionMap].getAllMultiPolygonFeatures[SomeProp]
    //Choose one MultiPolygon out of Feature Collection (ex. Great Plains, MO)
    val regionRDD: RDD[MultiPolygon] = sc.parallelize(Array(metroAll.get("48460").get.geom))

    //Rasterize Vector
    val geomLayerRDD: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = regionRDD.rasterize(1, geoTiff.cellType, ld)

    //Joined both RDDs by Spatial Key
    val joinedRasters = tiledRaster.join(geomLayerRDD)
    val zonalStatistics = joinedRasters.mapValues(x=> x._1.zonalStatisticsInt(x._2))
    //The Zonal Statistics returns a map with various stats for each value.
    val zonalStatisticsValues = zonalStatistics.values

    //Can expand this to other values.  Get max under Polygon.  With one zonal polygon, stats are calculated for value 1 (polygon) and -214747483648 (area outside polygon).
    //A bit unsure of the statistics surrounding in this step.  It might be more complex than taking the average of the averages/mean of the means for the tiles (?).
    val i = zonalStatisticsValues.map(x=> x(0))
    val l = i.map(x=> x.zmax)
    val maximum = l.max
    sc.stop()
  }

}

