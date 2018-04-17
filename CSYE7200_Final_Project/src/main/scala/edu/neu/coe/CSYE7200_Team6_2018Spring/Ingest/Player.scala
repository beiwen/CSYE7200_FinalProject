package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._


/**
  *  Created by team6 on 2018/3/19
  */

case class Player (game_size: Int, match_id: String, match_mode: String, party_size: Int,
                   player_assists: Int, player_dbno: Int, player_dist_ride: Double, player_dist_walk: Double,
                   player_dmg: Int, player_kills: Int, player_name: String, player_survive_time: Double,
                   team_id: Int, team_placement: Int)


object Player extends App{

  lazy val schema: StructType = new StructType(Array(StructField("date", StringType, true),
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


  lazy implicit val spark = {
    println("initial spark")
    SparkSession
      .builder()
      .appName("IngestRawToDataset")
      .master("local[*]")
      .getOrCreate()
  }
  /*
     To find encoder for type stored in a Dataset, Product types(case classes) are
     supported by importing spark.implicits._
  */
  import spark.implicits._
  class IngestPlayer extends Ingest{
    def ingest(srcDir: String, schema: StructType)(implicit spark:SparkSession): Dataset[Player] = {
      spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(schema)
        .format("csv")
        .load(srcDir)
        .as[Player]
    }
    def filterPlayers(ds: Dataset[Player]): Dataset[Player] = {
      ds.filter(d => (d.player_dist_ride != 0 || d.player_dist_walk != 0) && d.player_survive_time <= 2400)
    }
  }

  object IngestPlayer extends IngestPlayer
  val ds = IngestPlayer.ingest("sample.csv", schema)
  ds.show()
}

