package edu.neu.coe.CSYE7200_Team6_2018Spring.Usecases

import edu.neu.coe.CSYE7200_Team6_2018Spring.Classification.Classification
import edu.neu.coe.CSYE7200_Team6_2018Spring.Clustering.{ClusteringByBattle, ClusteringByDistance}
import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player
import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player.IngestPlayer
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType


object Usecases {

  /**
    * models object is used to fetch the different trained models saved in AWS S3 bucket.
    */
  object models {
    val nnModelForSolo = MultilayerPerceptronClassificationModel.load("s3a://csye7200.bucket.forfinal/Models/nnClassifierForSolo")
    val nnModelForDuo = MultilayerPerceptronClassificationModel.load("s3a://csye7200.bucket.forfinal/Models/nnClassifierForDuo")
    val nnModelForSquad = MultilayerPerceptronClassificationModel.load("s3a://csye7200.bucket.forfinal/Models/nnClassifierForSquad")

    val clusteringByDistForSolo = KMeansModel.load("s3a://csye7200.bucket.forfinal/Models/ClusteringByDistanceForSolo")
    val clusteringByDistForDuo = KMeansModel.load("s3a://csye7200.bucket.forfinal/Models/ClusteringByDistanceForDuo")
    val clusteringByDistForSquad = KMeansModel.load("s3a://csye7200.bucket.forfinal/Models/ClusteringByDistanceForSquad")

    val clusteringByBattleForSolo = KMeansModel.load("s3a://csye7200.bucket.forfinal/Models/ClusteringByBattleForSolo")
    val clusteringByBattleForDuo = KMeansModel.load("s3a://csye7200.bucket.forfinal/Models/ClusteringByBattleForDuo")
    val clusteringByBattleForSquad = KMeansModel.load("s3a://csye7200.bucket.forfinal/Models/ClusteringByBattleForSquad")
  }


  /**
    * Ingest data from path into dataset
    */
  val schema: StructType = Player.schema
  implicit val spark: SparkSession = Player.spark
  val path = "s3a://csye7200.bucket.forfinal/testData/*"
  val dataset: Dataset[Player] = IngestPlayer.ingest(path, schema)


  /** Use case 1 : Clustering
    * Before clustering, we need to filter the data by removing the unreasonable data and separate them by party_size
    */
  val filteredDSForClustering = IngestPlayer.filterPlayers(dataset)
  val soloPlayers = filteredDSForClustering.filter(d => d.party_size == 1).cache()
  val duoPlayers = filteredDSForClustering.filter(d => d.party_size == 2).cache()
  val squadPlayers = filteredDSForClustering.filter(d => d.party_size == 4).cache()

  /** Clustering1 --- Clustering by Battle
    *
    * Get an array of the DataFrame with prediction results of clustering by battle, respectively for solo matches, duo matches and squad matches
    */
  val inputCols1 = Array("player_dmg", "player_kills")
  val inputCols2 = Array("player_assists", "player_dbno", "player_dmg", "player_kills")
  //Get the dateframe with features column
  val soloDF_clustering_battle = ClusteringByBattle.createDfWithFeature(soloPlayers, inputCols1)
  val duoDF_clustering_battle = ClusteringByBattle.createDfWithFeature(duoPlayers, inputCols2)
  val squadDF_clustering_battle = ClusteringByBattle.createDfWithFeature(squadPlayers, inputCols2)
  //Get the prediction result of clustering by battle
  val solo_predicted_by_battle = models.clusteringByBattleForSolo.transform(soloDF_clustering_battle)
  val duo_predicted_by_battle = models.clusteringByBattleForDuo.transform(duoDF_clustering_battle)
  val squad_predicted_by_battle = models.clusteringByBattleForSquad.transform(squadDF_clustering_battle)
  Array(solo_predicted_by_battle, duo_predicted_by_battle, squad_predicted_by_battle)


  /** Clustering2 --- Clustering by Distance
    *
    * Get an array of the DataFrame with prediction results of clustering by distance, respectively for solo matches, duo matches and squad matches
    */
    val inputCols = Array("player_dist_ride", "player_dist_walk")
    //Get the dateframe with features column
    val soloDF_clustering_dist = ClusteringByDistance.createDfWithFeature(soloPlayers, inputCols)
    val duoDF_clustering_dist = ClusteringByDistance.createDfWithFeature(duoPlayers, inputCols)
    val squadDF_clustering_dist = ClusteringByDistance.createDfWithFeature(squadPlayers, inputCols)
    //Get the prediction result of clustering by distance
    val solo_predicted_by_dist = models.clusteringByDistForSolo.transform(soloDF_clustering_dist)
    val duo_predicted_by_dist = models.clusteringByDistForDuo.transform(duoDF_clustering_dist)
    val squad_predicted_by_dist = models.clusteringByDistForSquad.transform(squadDF_clustering_dist)
    Array(solo_predicted_by_dist, duo_predicted_by_dist, squad_predicted_by_dist)


  /** Use case 2 : Classification
    * Before classification, we need to filter the data by removing the unreasonable data and separate them by party_size
    */
    val filteredDSForClassification = Classification.filterPlayers(dataset)
    val solo_Players = filteredDSForClassification.filter(d => d.party_size == 1).cache()
    val duo_Players = filteredDSForClassification.filter(d => d.party_size == 2).cache()
    val squad_Players = filteredDSForClassification.filter(d => d.party_size == 4).cache()

    val soloDF_classification = Classification.createDfWithFeature(solo_Players)
    val duoDF_classification = Classification.createDfWithFeature(duo_Players)
    val squadDF_classification = Classification.createDfWithFeature(squad_Players)

    val solo_classification = models.nnModelForSolo.transform(soloDF_classification)
    val duo_classification = models.nnModelForDuo.transform(duoDF_classification)
    val squad_classification = models.nnModelForSquad.transform(squadDF_classification)
    Array(solo_classification, duo_classification, squad_classification)

}