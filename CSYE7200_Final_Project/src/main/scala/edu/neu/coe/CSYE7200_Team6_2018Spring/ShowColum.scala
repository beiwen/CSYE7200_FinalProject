package edu.neu.coe.CSYE7200_Team6_2018Spring

import org.apache.spark.{SparkConf, SparkContext}


object ShowColum extends App {



  def showCol(path: String): Seq[Option[String]] = {
    val sparkConf = new SparkConf().setAppName("showcol").setMaster("local")

    val sc = new SparkContext(sparkConf)
    val src = sc.textFile(path)
    val head = src.take(1)
    val res = head.toSeq.map(Option[String](_))
    res.map(println(_))
    return res
  }

  override def main(args: Array[String]): Unit = {
    showCol("s3://csye7200.bucket.forfinal/aggregate/agg_match_stats_4.csv")
  }
}
