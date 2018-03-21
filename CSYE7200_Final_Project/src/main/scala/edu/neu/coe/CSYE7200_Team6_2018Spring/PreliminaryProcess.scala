package edu.neu.coe.CSYE7200_Team6_2018Spring


import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

/**
  *  Created by team6 on 2018/3/20
  *  This module is for preliminary process, which includes reading schema from csv file,
  *  sampling 10000 rows from one of pubg datasource for unit test.
  */


object PreliminaryProcess extends App {

  override def main(args: Array[String]): Unit = {

    implicit val spark = {
      println("initial spark")
      SparkSession
        .builder()
        .appName("PreliminaryProcess")
        .master("local[*]")
        .getOrCreate()}

    //Setting up required configuration for spark to talk with AWS S3 bucket. The following way to provided AWS credential is not recommended.
    //Following code are provided just for users who have no experience with AWS S3.

    spark.conf.set("fs.s3n.awsAccessKeyId","AKIAJYRJRH6MYNFWM5CA")
    spark.conf.set("fs.s3n.awsSecretAccessKey", "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")

    val path = "s3n://csye7200.bucket.forfinal/aggregate/agg_match_stats_4.csv"

    lazy val src = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load(path)

    def showCol(): StructType = {
      val schema = src.schema
      println(schema)
      return schema
    }

    def sampling(rows: Int, schema: StructType):Unit = {

      val file = "temp"
      val desFile = "sample.csv"
      FileUtil.fullyDelete(new File(file))
      FileUtil.fullyDelete(new File(desFile))

      val sampleRows = src.limit(rows)
      sampleRows.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("temp")

      def merge(srcPath: String, dstPath: String): Unit =  {
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
      }

      merge(file, desFile)
      FileUtil.fullyDelete(new File(file))
    }

    val s = showCol()
    sampling(10000, s)
  }
}
