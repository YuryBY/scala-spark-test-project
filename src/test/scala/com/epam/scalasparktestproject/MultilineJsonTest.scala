package com.epam.scalasparktestproject

import java.io.File
import java.nio.file.Files

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

class MultilineJsonTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("multiline-json test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val JSON_PATH = "src/test/resources/json_example.json"
  val MULTILINE_JSON_PATH = "src/test/resources/multiline_json_example.json"
  val STACK_S__JSON_PATH = "src/test/resources/stack_s_json_example.json"
  val DATABRICKS_JSON_PATH = "src/test/resources/databricks_json_example.json"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read multiline json") {
    val jsonRDD = sc.textFile(DATABRICKS_JSON_PATH)
    jsonRDD.collect().foreach(println)
  }

}
