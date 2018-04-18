package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}


class IngestSpec extends FlatSpec with Matchers {

  behavior of "Ingest"

  it should "work for Int" in {
    val schema: StructType = new StructType(Array(StructField("number", IntegerType, true)))
    val path = "testForInt.csv"
    implicit val spark = {
      SparkSession
        .builder()
        .appName("IngestRawToDataset")
        .master("local[*]")
        .getOrCreate()
    }
    trait IngestInt extends Ingest {
      import spark.implicits._
      def ingest(srcDir: String, schema: StructType)(implicit spark:SparkSession): Dataset[Int] = {
        spark.read
          .option("header", "true")
          .option("inferSchema", "false")
          .schema(schema)
          .format("csv")
          .load(srcDir)
          .as[Int]
      }
    }
    object IngestInt extends IngestInt
    val ids = IngestInt.ingest(path,schema)
    ids.count() shouldBe 14
    ids.distinct().count() shouldBe 10
    val total = ids.collect().foldLeft(0)(_ + _)
    total shouldBe 516
  }
}