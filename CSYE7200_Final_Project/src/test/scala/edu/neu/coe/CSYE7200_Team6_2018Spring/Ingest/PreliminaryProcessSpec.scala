package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.scalatest.{FlatSpec, Matchers}

class PreliminaryProcessSpec extends FlatSpec with Matchers {

  val preliminaryProcess = new PreliminaryProcess
  val df = preliminaryProcess.readSrc("csv","sample.csv")

  behavior of "Priliminary Process"

  it should "work for reading source" in {
    df shouldBe a [DataFrame]
    df.count() shouldBe 10000
  }

  it should "work for Schema(Column)" in {
    val schema = df.schema
    val names = for(schemafield <- schema) yield schemafield.name
    val types = for(schemafield <- schema) yield schemafield.dataType
    names === List("date", "game_size", "match_id", "match_mode", "party_size", "player_assists", "player_dbno", "player_dist_ride", "player_dist_walk", "player_dmg", "player_kills", "player_name", "player_survive_time", "team_id", "team_placement")
    types === List(StringType, IntegerType, StringType, StringType, IntegerType, IntegerType, IntegerType, DoubleType, DoubleType, IntegerType, IntegerType, StringType, DoubleType, IntegerType, IntegerType)
  }

  it should "work for Sampling" in {
    preliminaryProcess.sampling(50, df, "testOfSampling.csv")
    assert(Files.exists(Paths.get("testOfSampling.csv")))
    val src = preliminaryProcess.readSrc("csv", "testOfSampling.csv")
    src.count() shouldBe 50
  }
}
