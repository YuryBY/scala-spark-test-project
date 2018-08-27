package main.scala.com.epam.scalasparktestproject

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Yury_Bakhmutski on 8/27/2018.
  */
class MultilineJsonReader {

  def parseMultilineJson(sc: SparkContext, bidsPath: String): RDD[String] = {
    sc.textFile(bidsPath)
  }

}
