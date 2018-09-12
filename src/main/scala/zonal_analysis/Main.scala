package zonal_analysis

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
import local_analysis.rasterdatasets.myRaster
//Has TileLayout Object, MultibandTile
import geotrellis.raster.io.geotiff._
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
//File Object
import scala.io.StdIn
import java.io.File



object Main {

  def main(args: Array[String]): Unit = {

    val tilesizes = Array(25, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000) //, 1500, 2000, 2500, 3000, 3500, 4000)

    //Raster Layer
    val rasterDatasets = List(
      new myRaster("glc", "/home/david/Downloads/glc2000.tif", 16, 1, 4326)
    //new myRaster("glc", "/data/projects/G-818404/glc2000_clipped.tif", 16, 1),
    //new myRaster("meris", "/data/projects/G-818404/meris_2010_clipped.tif", 100, 1),
    //new myRaster("nlcd", "/data/projects/G-818404/nlcd_2006.tif", 21, 1)
    //new rasterdataset("meris_3m", "/data/projects/G-818404/meris_2010_clipped_3m/", 100, 1)
    )

    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Tiler").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)

    for(r<-rasterDatasets){
      //val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(r.thePath)
      val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(r.thePath, HadoopGeoTiffRDD.Options.DEFAULT)
      val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(250))

      // val rasterRDD: RDD[(ProjectedExtent, geotrellis.raster.Tile)] = sc.hadoopGeoTiffRDD(r.thePath)
      // val geoTiff: SinglebandGeoTiff = GeoTiffReader.readSingleband(r.thePath, decompress = false, streaming = true)
      // val ld = LayoutDefinition(geoTiff.rasterExtent, 100)
      val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout)


    }

/*

        //Vector Layer
        val file: String = "/home/david/shapefiles/4326/states_2.geojson" //"data/censusMetroNew.geojson"
        val region_files = scala.io.Source.fromFile(file).getLines.mkString
        case class Attributes(NAME: String,LSAD: String,AFFGEOID: String,ALAND: Int, AWATER: Int, ID: Int)
        implicit val boxedToRead = jsonFormat6(Attributes)

        val theRegion: Map[String, MultiPolygonFeature[Attributes]] = region_files.parseGeoJson[JsonFeatureCollectionMap].getAllMultiPolygonFeatures[Attributes]
        //Choose one MultiPolygon out of Feature Collection (ex. Great Plains, MO)

        val regionKeys = theRegion.key.toList

        for (i<-0 to regionKeys.length-1){
          println(i)
          val regionRDD: RDD[MultiPolygon] = sc.parallelize(Array(theRegion.get(regionKeys(i).toString).get.geom))
        }

        val regionRDD: RDD[MultiPolygon] = sc.parallelize(Array(theRegion.get(polyKeys(0)).get.geom))
        //Rasterize Vector
        val geomLayerRDD: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = regionRDD.rasterize(1, rasterMetaData.cellType, rasterMetaData.layout)


        //Joined both RDDs by Spatial Key
        val joinedRasters = tiledRaster.join(geomLayerRDD)
        val zonalStatistics = joinedRasters.mapValues(x=> x._1.zonalStatisticsInt(x._2))
        //The Zonal Statistics returns a map with various stats for each value.
        val zonalStatisticsValues = zonalStatistics.values

        //Can expand this to other values.  Get max under Polygon.  With one zonal polygon, stats are calculated for value 1 (polygon) and -214747483648 (area outside polygon).
        //A bit unsure of the statistics surrounding in this step.  It might be more complex than taking the average of the averages/mean of the means for the tiles (?).
        val i = zonalStatisticsValues.map(x=> x(0))
        val l = i.map(x=> x.zmax)
2        val maximum = l.max

*/

    sc.stop()
  }

}
