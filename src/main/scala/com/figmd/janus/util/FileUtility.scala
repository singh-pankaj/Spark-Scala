package com.figmd.janus.util

import java.util.Properties

import scala.io.Source

class FileUtility extends Serializable {

  def getProperty(property: String): String = {

    var properties: Properties = null
    val url = getClass.getResource("/application.properties")


    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }

    return properties.getProperty(property);

  }

  def getProperty():Properties = {

    var properties: Properties = null
    val url = getClass.getResource("/application.properties")

    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }

    return properties

  }


}
