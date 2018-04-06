package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}


object Ingest {

  val schema: StructType = new StructType(Array(StructField("date", StringType, true),
    StructField("game_size", IntegerType, true),
    StructField("match_id",StringType, true),
    StructField("match_mode", StringType, true),
    StructField("party_size", IntegerType, true),
    StructField("player_assists", IntegerType, true),
    StructField("player_dbno",IntegerType,true),
    StructField("player_dist_ride",DoubleType,true),
    StructField("player_dist_walk",DoubleType,true),
    StructField("player_dmg",IntegerType,true),
    StructField("player_kills",IntegerType,true),
    StructField("player_name",StringType,true),
    StructField("player_survive_time",DoubleType,true),
    StructField("team_id",IntegerType,true),
    StructField("team_placement",IntegerType,true))
  )

  def ingest(spark: SparkSession, srcDir: String, schema: StructType): Dataset[Player]  = {

    import spark.implicits._

    spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .format("csv")
      .load(srcDir)
      .as[Player]
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = {
      println("initial spark")
      SparkSession
        .builder()
        .appName("IngestRawToDataset")
        .master("local[*]")
        .getOrCreate()
    }

    val ds = ingest(spark, "sample.csv", schema)
    ds.show()

  }

}
