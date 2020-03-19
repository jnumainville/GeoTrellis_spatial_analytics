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
import datasets.vectordatasets.myVector
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
import java.io._
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}

// config
import collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}

object Main {

  def main(args: Array[String]): Unit = {
    /*
    Entry point for the zonal analysis

    Input:
      None

    Output:
      None
    */

    val config: Config = ConfigFactory.load("datasets.conf")

    val dataName = config.getStringList("Main.dataName").asScala.toList
    val dataFile = config.getStringList("Main.dataFile").asScala.toList
    val dataPixelVal = config.getIntList("Main.dataPixelVal").asScala.toList
    val dataNewPixel = config.getIntList("Main.dataNewPixel").asScala.toList

    val vectorName = config.getStringList("Main.vectorName").asScala.toList
    val vectorFile = config.getStringList("Main.vectorFile").asScala.toList
    val vectorJSON = config.getStringList("Main.vectorJSON").asScala.toList

    val tileSizes = config.getIntList("Main.tilesizes").asScala.toList

    val outCSVPath = config.getString("Main.outCSVPath")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //Raster Dataset Path
    val rasterDatasets = for {
      n <- dataName
      f <- dataFile
      v <- dataPixelVal
      p <- dataNewPixel
    } yield myRaster(n, f, v, p)

    val vectorDatasets = for {
      n <- vectorName
      f <- vectorFile
      j <- vectorJSON
    } yield myRaster(n, f, j)

    val writer = new PrintWriter(new File(outCSVPath))
    writer.write("analytic,raster_dataset,tilesize,vector_dataset,total_time,multipolygon_time, polygon_time, run\n")

    val conf = new SparkConf().setMaster("local[12]").setAppName("Zonal Stats").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    implicit val sc = new SparkContext(conf)

    for(r<-rasterDatasets) {

      for (tilesize <- tileSizes) {

        val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(r.thePath,
          HadoopGeoTiffRDD.Options.DEFAULT)
        val (_, rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(tilesize))
        val tiledRaster: RDD[(SpatialKey, geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType,
          rasterMetaData.layout)
        val rasterTileLayerRDD: TileLayerRDD[SpatialKey] = ContextRDD(tiledRaster, rasterMetaData)
        var rasterName = ""
        var vectorName = ""
        //Removing other RDD
        rasterRDD.unpersist()
        tiledRaster.unpersist()
        for (theRun <- 1 to 3) {
          for (v <- vectorDatasets) {
            var jsonPath = v.theBasePath + "/" + r.srid + "/" + v.theJSON
            println(jsonPath)
            val theJSON = scala.io.Source.fromFile(jsonPath).getLines.mkString
            case class Attributes(NAME: String, ID: Int)
            implicit val boxedToRead = jsonFormat2(Attributes)

            // GeoTrellis does not handle both multipolygons and polgyons in the same function
            val multiPolygons: Map[String, MultiPolygonFeature[Attributes]] = theJSON.
              parseGeoJson[JsonFeatureCollectionMap].getAllMultiPolygonFeatures[Attributes]
            val polygons: Map[String, PolygonFeature[Attributes]] = theJSON.
              parseGeoJson[JsonFeatureCollectionMap].getAllPolygonFeatures[Attributes]

            //Potential object for writing outvalues
            var ZonalStats = new ListBuffer[Map[String, (Int, Int, Double)]]()

            var zonalStatsStart = System.currentTimeMillis()

            val theMultiPolygonsKeys = multiPolygons.keys.toList
            for (i<-0 to theMultiPolygonsKeys.length-1){

              var geom = multiPolygons.get(theMultiPolygonsKeys(i).toString).get.geom
              var histogram = rasterTileLayerRDD.polygonalHistogram(geom)
              var theStats = histogram.statistics

              println(theMultiPolygonsKeys(i).toString, theStats)

            }
            

            var zonalStatsStop = System.currentTimeMillis()
            var multiPolygonTime = zonalStatsStop - zonalStatsStart
            println("Time to complete multipolygons: ", multiPolygonTime)
            println("*********** Finished multipolygons ***************")

            zonalStatsStart = System.currentTimeMillis()

            val thePolygonsKeys = polygons.keys.toList
            for (i<-0 to thePolygonsKeys.length-1){

              var geom = polygons.get(thePolygonsKeys(i).toString).get.geom
              var histogram = rasterTileLayerRDD.polygonalHistogram(geom)
              var theStats = histogram.statistics
              println(thePolygonsKeys(i).toString, theStats)

            }
        
            zonalStatsStop = System.currentTimeMillis()
            var polygonTime = zonalStatsStop - zonalStatsStart
            println("Time to complete polygons: ", polygonTime)
            var totalTime = polygonTime + multiPolygonTime
            rasterName = r.name
            vectorName = v.name

            println(s"Total Time to complete: $totalTime for $rasterName with tilesize $tilesize on vector $vectorName")

            writer.write(s"polygonal_summary,$rasterName,$tilesize,$vectorName,$totalTime,$multiPolygonTime, " +
              s"$polygonTime, $theRun\n")


          } //vector
        } // run
      }//tile
    }//raster

    writer.close()
    sc.stop()
  }

}
