package com.epam.hubd.spark.scala.core.homework

import java.io.File
import java.nio.file.Files

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.apache.spark.SparkConf

class MultilineJsonTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val JSON_PATH = "src/test/resources/json_example.json"
  val MULTILINE_JSON_PATH = "src/test/resources/multiline_json_example.json"
  val STACK_S__JSON_PATH = "src/test/resources/stack_s_json_example.json"
  val DATABRICKS__JSON_PATH = "src/test/resources/databricks_json_example.json"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read multiline json") {
    val expected = sc.parallelize(
      Seq(
        "0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35",
        "0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL"
      )
    )

    //val rawBids = MultilineJsonReader.parseMultilineJson(sc, DATABRICKS__JSON_PATH)
    val jsonRDD = sc.textFile(DATABRICKS__JSON_PATH)
    jsonRDD.collect().foreach(println)

    //assertRDDEquals(expected, rawBids)
  }

}
