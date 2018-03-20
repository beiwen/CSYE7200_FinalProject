package edu.neu.coe.CSYE7200_Team6_2018Spring

import org.apache.spark.sql.SparkSession


object ShowColum extends App {

  override def main(args: Array[String]): Unit = {
    implicit val spark = {
      println("initial spark")
      SparkSession
        .builder()
        .appName("ShowCol")
        .master("local[*]")
        .getOrCreate()}


    spark.conf.set("fs.s3n.awsAccessKeyId","AKIAJYRJRH6MYNFWM5CA")
    spark.conf.set("fs.s3n.awsSecretAccessKey", "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")

    def showCol(path: String): Seq[Option[String]] = {
      val src = spark.read.textFile(path)
      val head = src.take(1)
      val res = head.toSeq.map(Option[String](_))
      res.map(println(_))
      return res
    }

    showCol("s3n://csye7200.bucket.forfinal/aggregate/agg_match_stats_4.csv")
  }
}
