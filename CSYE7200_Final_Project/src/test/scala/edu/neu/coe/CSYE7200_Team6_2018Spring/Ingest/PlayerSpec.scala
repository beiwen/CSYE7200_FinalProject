package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player.IngestPlayer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class PlayerSpec extends FlatSpec with Matchers {

  val schema: StructType = Player.schema
  implicit val spark: SparkSession = Player.spark
  val dataset: Dataset[Player] = IngestPlayer.ingest("sample.csv", schema)

  behavior of "PlayerIngest"

  it should "work for Player_Kills" in {
    val killsNumList = for (p <- dataset take (20)) yield p.player_kills
    val res = for (k <- killsNumList; if k == 1) yield k
    res.size shouldBe 5
  }

  it should "work for Player object" in {
    dataset.first() should matchPattern {
      case Player(95, "2U4GBNA0YmmhivBOFUiipklIPVdC0DRgkX88eyhTYGiAnoXrjvEhNQHZMWi8d5y9", "tpp", 1, 0, 0, 0.0, 37.919838, 20, 0, "m3xdave", 106.351, 100000, 88) =>
    }
  }

  it should "work for the number of Player" in {
    dataset.count() shouldBe 10000
  }

  behavior of "filter"
  it should "work for filtering unusual players" in{
    val filteredPL = IngestPlayer.filterPlayers(dataset)
    filteredPL.count() shouldBe 9779
  }
}


