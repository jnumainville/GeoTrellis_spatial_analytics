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
import datasets.rasterdatasets.myRaster
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
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}


object Main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //Raster Dataset Path
    val rasterDatasets = List(
      new myRaster("glc", "/home/david/Downloads/glc2000.tif", 16, 1, 4326)
    //new myRaster("glc", "/data/projects/G-818404/glc2000_clipped.tif", 16, 1),
    //new myRaster("meris", "/data/projects/G-818404/meris_2010_clipped.tif", 100, 1),
    //new myRaster("nlcd", "/data/projects/G-818404/nlcd_2006.tif", 21, 1)
    //new rasterdataset("meris_3m", "/data/projects/G-818404/meris_2010_clipped_3m/", 100, 1)
    )

    val vectorDatasets = List(
      ("states", "/home/david/shapefiles/4326/states_2.geojson")
    )

    val tileSizes = Array(25, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000) //, 1500, 2000, 2500, 3000, 3500, 4000)

    val outSummaryStats = "/home/david/geotrellis_glc_stats_zonalstats.csv"
    //val writer = new BufferedWriter(new )

    val conf = new SparkConf().setMaster("local[2]").setAppName("Zonal Stats").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)

    for(r<-rasterDatasets){
//      for (tile <- tileSizes){

        //val geoTiff: SinglebandGeoTiff = SinglebandGeoTiff(r.thePath)
        val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(r.thePath, HadoopGeoTiffRDD.Options.DEFAULT)
        val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(250))
        val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout)
        val rasterTileLayerRDD: TileLayerRDD[SpatialKey] = ContextRDD(tiledRaster, rasterMetaData)
        //number of partitions RDD.keys.toList.length

        rasterRDD.unpersist()
        tiledRaster.unpersist()
        // for(v<-vectorDatasets){}
        // val file: String = "/home/david/shapefiles/4326/states_2.geojson" //"data/censusMetroNew.geojson"
        val jsonPath  = vectorDatasets(0)._2
        val theJSON = scala.io.Source.fromFile(jsonPath).getLines.mkString
        case class Attributes(NAME: String,LSAD: String,AFFGEOID: String,ALAND: Int, AWATER: Int, ID: Int)
        implicit val boxedToRead = jsonFormat6(Attributes)

        // This needs to be 2 separate functions
        val multiPolygons: Map[String, MultiPolygonFeature[Attributes]] = theJSON.parseGeoJson[JsonFeatureCollectionMap].getAllMultiPolygonFeatures[Attributes]
        val polygons: Map[String, PolygonFeature[Attributes]] = theJSON.parseGeoJson[JsonFeatureCollectionMap].getAllPolygonFeatures[Attributes]


        //def MultiPolygonSummaryStats(mp: Map[String,geotrellis.vector.MultiPolygonFeature[Attributes]] :org.apache.spark.rdd.RDD[(geotrellis.spark.SpatialKey, geotrellis.raster.Tile)], oldValue:Int, newValue:Int)  = {

        val theMultiPolygonKeys = multiPolygons.keys.toList
        val thePolygonKeys = polygons.keys.toList

        var ZonalStats = new ListBuffer[Map[String, (Int, Int, Double)]]()

        var zonalStatsStart = System.currentTimeMillis()

        var geometries = multiPolygons.mapValues(x=> x.geom)
        var histogram = geometries.mapValues(x => rasterTileLayerRDD.polygonalHistogram(x))
        val multiPolyStats = histogram.mapValues(x => x.statistics.toList)  //count.min._1)
        for( k <- multiPolyStats.keys) {
          println(multiPolyStats(k))
        }

/*        for (i<-0 to theMultiPolygonKeys.length-1){

          //var geom = multiPolygons.get(theMultiPolygonKeys(0).toString).get.geom
          var geom = multiPolygons.get(theMultiPolygonKeys(i).toString).get.geom
          var histogram = rasterTileLayerRDD.polygonalHistogram(geom)
          var theStats = histogram.statistics
          //var theMean = histogram.mean.min
          println(theMultiPolygonKeys(i).toString, theStats)

          //ZonalStats += Map(theMultiPolygonKeys(i).toString -> (theMin, theMax, theMean))
          //Map("x" -> 24, "y" -> 25, "z" -> 26)

        }*/


        var zonalStatsStop = System.currentTimeMillis()
        var finalTime = zonalStatsStop-zonalStatsStart
        println("Time to complete: ", finalTime)
        println("*********** Finished multipolygons ***************")

        zonalStatsStart = System.currentTimeMillis()
        geometries = polygons.mapValues(x=> x.geom)
        histogram = geometries.mapValues(x => rasterTileLayerRDD.polygonalHistogram(x))
        //val polygonMeans = histogram.mapValues(x => x.mean.min)
        //val polyStats = histogram.mapValues(x => x.statistics)
        //val (polyMin, polyMax) = histogram.mapValues(x => x.minMaxValues.min)
        //val polyMin = histogram.mapValues(x => x.minMaxValues.min._1)
        //val polyMax = histogram.mapValues(x => x.minMaxValues.min._2)
        val polyStats = histogram.mapValues(x => x.statistics.toList)  //count.min._1)
        for( k <- polyStats.keys) {
          println(polyStats(k))
        }
        //val polyStats = polygonMeans.toList ++ polyMinMax.toList
        //val polys = polyStats.groupBy(_._1).map{case(k, v) => k -> v.map(_._2).toSeq}





/*        for (i<-0 to thePolygonKeys.length-1){

          var geom = polygons.get(thePolygonKeys(i).toString).get.geom
          var histogram = rasterTileLayerRDD.polygonalHistogram(geom)
          var(theMin, theMax) = histogram.minMaxValues.min
          var theMean = histogram.mean.min
          println(thePolygonKeys(i).toString, theMin, theMax, theMean)

          ZonalStats += Map(thePolygonKeys(i).toString -> (theMin, theMax, theMean))


        }*/

        zonalStatsStop = System.currentTimeMillis()
        finalTime = zonalStatsStop-zonalStatsStart
        println("Time to complete: ", finalTime)
/*        val multiPolygonStats = ZonalStats.toList
        val thePolygonKeys = polygons.keys.toList*/
        // ZonalStats.toList.foreach(writer.write)

      }

      //      }




//    }


    sc.stop()
  }

}
