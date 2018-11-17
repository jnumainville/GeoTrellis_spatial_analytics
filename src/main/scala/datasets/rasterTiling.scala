package datasets

import datasets.rasterdatasets.myRaster
import geotrellis.raster.Tile
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//Has TileLayout Object, MultibandTile
import geotrellis.raster.io.geotiff._

object rasterTiling {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val rasterDatasets = List(
      new myRaster("glc", "/media/sf_data/scidb_datasets/glc2000_clipped.tif", 16, 1)
      /* new myRaster("glc", "/data/projects/G-818404/glc2000_clipped.tif", 16, 1),
      new myRaster("meris", "/data/projects/G-818404/meris_2010_clipped.tif", 100, 1),
      new myRaster("nlcd", "/data/projects/G-818404/nlcd_2006.tif", 21, 1) */
      // new myRaster("meris_3m", "/data/projects/G-818404/meris_3m/", 100, 1)
    )


    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Tiler").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.regisintrator", "geotrellis.spark.io.kryo.KryoRegistrator")//.set("spark.driver.memory", "2g").set("spark.executor.memory", "1g")
    implicit val sc = new SparkContext(conf)

    val r = rasterDatasets(0)
    println(r.thePath)
    val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(r.thePath), HadoopGeoTiffRDD.Options.DEFAULT)

    //val rasterRDD: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial(new Path(r.thePath), HadoopGeoTiffRDD.Options.DEFAULT)
    val pValue = r.pixelValue

    val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD,FloatingLayoutScheme(tileSize = 300))

    //val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout)


    //val (_,rasterMetaData) = TileLayerMetadata.fromRdd(rasterRDD, FloatingLayoutScheme(300))
    //val ld = LayoutDefinition(geoTiff.rasterExtent, tilesize)
    //val tiledRaster: RDD[(SpatialKey,geotrellis.raster.Tile)] = rasterRDD.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout)
    sc.stop()
  }
}
