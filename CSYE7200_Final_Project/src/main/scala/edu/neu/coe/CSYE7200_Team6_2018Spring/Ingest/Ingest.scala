package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Ingest {

//  def ingest(spark: SparkSession, srcDir: String): Dataset[Row]  = {
//    spark.
//  }

  def main(args: Array[String]): Unit = {
    implicit val spark = {
      println("initial spark")
      SparkSession
        .builder()
        .appName("IngestRawToDataset")
        .master("local[*]")
        .getOrCreate()
    }



  }

}
