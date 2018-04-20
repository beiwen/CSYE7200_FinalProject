package edu.neu.coe.CSYE7200_Team6_2018Spring.Clustering

import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player
import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player.IngestPlayer
import org.scalatest.{FlatSpec, Matchers}

class ClusteringByBattleSpec extends FlatSpec with Matchers {

  val schema = Player.schema
  implicit val spark = Player.spark
  val dataset = IngestPlayer.ingest("sample.csv", schema)
  val filteredDS = IngestPlayer.filterPlayers(dataset)

  behavior of "Clustering by Battle"
  it should "work for solo match" in {
    val soloPlayers = filteredDS.filter(d => d.party_size == 1).cache()
    val inputCols1 = Array("player_dmg","player_kills")
    val soloModel = ClusteringByBattle.clusteringHelper(soloPlayers)
    soloModel.clusterCenters should have length 5
    val soloDF = ClusteringByBattle.createDfWithFeature(soloPlayers,inputCols1)
    val soloClustered = soloModel.transform(soloDF)
    soloClustered.columns.length shouldBe 6
  }

  it should "work for duo match and squad match" in {
    val duoPlayers = filteredDS.filter(d => d.party_size == 2).cache()
    val squadPlayers = filteredDS.filter(d => d.party_size == 4).cache()
    val inputCols2 = Array("player_assists","player_dbno","player_dmg","player_kills")
    val duoModel = ClusteringByBattle.clusteringHelper(duoPlayers)
    val squadModel = ClusteringByBattle.clusteringHelper(squadPlayers)
    duoModel.clusterCenters should have length 5
    squadModel.clusterCenters should have length 4
    val duoDF = ClusteringByBattle.createDfWithFeature(duoPlayers,inputCols2)
    val squadDF = ClusteringByBattle.createDfWithFeature(squadPlayers,inputCols2)
    duoDF.columns should equal (squadDF.columns)
  }
}
