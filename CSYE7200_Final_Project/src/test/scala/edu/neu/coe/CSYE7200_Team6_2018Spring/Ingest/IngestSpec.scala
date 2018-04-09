package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class IngestSpec extends FlatSpec with Matchers {

//  behavior of "ingest"
//  it should "work for Player" in {
//    val src = "sample.csv"
//    implicit val spark = {
//      println("initial spark")
//      SparkSession
//        .builder()
//        .appName("IngestRawToDataset")
//        .master("local[*]")
//        .getOrCreate()
//    }
//    val dataset = Ingest.ingest(spark, src, Ingest.schema)
//    val killsNumList = for (p <- dataset take (20)) yield p.player_kills
//    val res = for (k <- killsNumList; if k == 1) yield k
//    res.size shouldBe 5
//  }
}