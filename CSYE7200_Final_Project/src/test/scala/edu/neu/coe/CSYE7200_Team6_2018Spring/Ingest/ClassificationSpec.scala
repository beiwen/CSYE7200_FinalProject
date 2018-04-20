package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player.IngestPlayer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.scalatest.{FlatSpec, Matchers}

class ClassificationSpec extends FlatSpec with Matchers{

  val schema: StructType = Player.schema
  implicit val spark: SparkSession = Player.spark
  val dataset: Dataset[Player] = IngestPlayer.ingest("sample.csv", schema)

  behavior of "Classification"

  it should "work for NNModels" in {
    val nnModels = Classification.buildingNNModels(dataset.limit(10))
    nnModels.size shouldBe 3
    val arr1 = nnModels.map(m => m.getClass.toString.equals("class org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel"))
//    nnModels(0).getClass shouldBe MultilayerPerceptronClassificationModel.getClass()
    arr1 shouldBe Array(true, true, true)
  }

  it should "work for RFModels" in {
    val rfModels = Classification.buildingRFModels(dataset)
    rfModels.size shouldBe 3
    val arr2 = rfModels.map(m => m.getClass.toString.equals("class org.apache.spark.ml.classification.RandomForestClassificationModel"))
    arr2 shouldBe Array(true, true, true)
  }

  it should "work for createDfWithFeatures" in {
    val df = Classification.createDfWithFeature(dataset)
    df.select("features", "survived").head().size shouldBe 2
  }

  it should "work for filterPlayers" in {
    val filteredPlayers = Classification.filtPlayers(dataset)
    filteredPlayers.filter(d => d.team_placement == 0).count() shouldBe 0
    filteredPlayers.filter(d => d.player_dist_ride == 0 && d.player_dist_walk == 0).count shouldBe 0
    filteredPlayers.filter(d => d.player_survive_time > 2400).count shouldBe 0
  }
}
