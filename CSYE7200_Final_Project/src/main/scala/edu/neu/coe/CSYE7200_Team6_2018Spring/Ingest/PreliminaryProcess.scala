package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

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

class PreliminaryProcess {
  //used to read data from a specific path(s3 bucket) into DataFrame,
  //because need to infer schema, so it is relatively slow
  val ACCESSKEYID = ""
  val SECRETKEY =""
  def readSrc(format : String, path : String) = {
    implicit val spark = {
      println("initial spark")
      SparkSession
        .builder()
        .appName("PreliminaryProcess")
        .master("local[*]")
        .getOrCreate()
    }
    //Setting up required configuration for spark to talk with AWS S3 bucket. The following way to provided AWS credential is not recommended.
    //Following code are provided just for users who have no experience with AWS S3.
    spark.conf.set("fs.s3n.awsAccessKeyId", ACCESSKEYID)
    spark.conf.set("fs.s3n.awsSecretAccessKey", SECRETKEY)
    spark.read.format(format)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

  }
  //used to show the dataFrame' s schema
  def showCol(src : DataFrame): StructType = {
    val schema = src.schema
    println(schema)
    return schema
  }
  //used to merge files from an HDFS directory over to an HDFS file
  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  //used to take a sample with the specific size from source DataFrame and merge into destination file through merge function
  def sampling(rows: Int, src: DataFrame, desfile :String):Unit = {
    val file = "temp"
    val desFile = desfile
    FileUtil.fullyDelete(new File(file))
    FileUtil.fullyDelete(new File(desFile))

    val sampleRows = src.limit(rows)
    sampleRows.coalesce(1).write.option("header", "true").format("com.databricks.spark.csv").save("temp")

    merge(file, desFile)
    FileUtil.fullyDelete(new File(file))
  }
}


object PreliminaryProcess extends App {
  val path = "s3n://csye7200.bucket.forfinal/aggregate/agg_match_stats_4.csv"
  val preliminaryProcess = new PreliminaryProcess
  val src = preliminaryProcess.readSrc("csv", path)
  preliminaryProcess.showCol(src)
  preliminaryProcess.sampling(10000, src, "sample.csv")
}
