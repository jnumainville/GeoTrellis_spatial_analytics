package datasets

object rasterdatasets {
  case class myRaster(name: String, thePath: String, pixelValue: Int, newPixel: Int, srid: Int=4326)
}

object vectordatasets{
  case class myVector(name: String, theBasePath: String, theJSON: String, srid:Int=4326)
}
