package edu.neu.coe.CSYE7200_Team6_2018Spring.Clustering

import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player
import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player.IngestPlayer
import org.scalatest.{FlatSpec, Matchers}

class ClusteringByDistanceSpec extends FlatSpec with Matchers {

  val inputCols = Array("player_dist_ride", "player_dist_walk")
  val schema = Player.schema
  implicit val spark = Player.spark
  val dataset = IngestPlayer.ingest("sample.csv", schema)
  val filteredDS = IngestPlayer.filterPlayers(dataset)

  behavior of "Clustering by Distance"

  it should "work for solo match" in {
    val soloPlayers = filteredDS.filter(d => d.party_size == 1).cache()
    val soloModel = ClusteringByDistance.clusteringHelper(soloPlayers)
    soloModel.clusterCenters should have length 6
    val soloDF = ClusteringByDistance.createDfWithFeature(soloPlayers,inputCols)
    val soloClustered = soloModel.transform(soloDF)
    soloClustered.createOrReplaceTempView("soloDist")
    soloClustered.map(_.getAs())
    assert(soloClustered.columns.length === 6)
    soloClustered.columns contains ("prediction")
  }

  it should "work for duo match" in {
    val duoPlayers = filteredDS.filter(d => d.party_size == 2).cache()
    val duoModel = ClusteringByDistance.clusteringHelper(duoPlayers)
    duoModel.clusterCenters should have length 4
    val duoDF = ClusteringByDistance.createDfWithFeature(duoPlayers,inputCols)
    duoDF.columns.length shouldBe 5
    val duoClustered = duoModel.transform(duoDF)
    val predictions = for(row <- duoClustered.select("prediction").collect()) yield row(0)
    predictions.foreach(_.toString should contain oneOf('0','1','2','3'))
  }

  it should "word for squad match" in{
    val squadPlayers = filteredDS.filter(d => d.party_size == 4).cache()
    val squadModel = ClusteringByDistance.clusteringHelper(squadPlayers)
    squadModel.clusterCenters should have length 5
  }
}
