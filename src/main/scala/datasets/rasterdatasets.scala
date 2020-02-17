package datasets


object rasterdatasets {
  /*
  Class to store raster dataset information

  Input:
    name = A string containing the name of the raster
    thePath = A string containing the path where the raster is located
    pixelValue = An integer containing the value of the pixel
    newPixel = An integer containing the new pixel
    srid = An integer containing the spatial reference identifier

  Output:
    An instance of the myRaster class
  */
  case class myRaster(name: String, thePath: String, pixelValue: Int, newPixel: Int, srid: Int=4326)
}

object vectordatasets{
  /*
  Class to store vector dataset information

  Input:
    name = A string containing the name of the vector
    theBasePath = A string containing the path where the vector is located
    theJSON = A string containing the JSON
    srid = An integer containing the spatial reference identifier

  Output:
    An instance of the myVector class
  */
  case class myVector(name: String, theBasePath: String, theJSON: String, srid:Int=4326)
}
