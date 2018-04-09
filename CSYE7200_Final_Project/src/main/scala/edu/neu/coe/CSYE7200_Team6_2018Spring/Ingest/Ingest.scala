package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}


trait Ingest{
  def ingest(srcDir: String, schema: StructType)(implicit spark:SparkSession): Dataset[_]
}


