package edu.neu.coe.CSYE7200_Team6_2018Spring.Usecases

import edu.neu.coe.CSYE7200_Team6_2018Spring.Classification.Classification
import edu.neu.coe.CSYE7200_Team6_2018Spring.Clustering.{ClusteringByBattle, ClusteringByDistance}
import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player
import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player.IngestPlayer
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType


object Usecases extends App {

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
  //Get the DataFrame with features column
  val soloDFClusteringBattle = ClusteringByBattle.createDfWithFeature(soloPlayers, inputCols1)
  val duoDFClusteringBattle = ClusteringByBattle.createDfWithFeature(duoPlayers, inputCols2)
  val squadDFClusteringBattle = ClusteringByBattle.createDfWithFeature(squadPlayers, inputCols2)
  //Get the prediction result of clustering by battle
  val soloPredictedByBattle = models.clusteringByBattleForSolo.transform(soloDFClusteringBattle)
  val duoPredictedByBattle = models.clusteringByBattleForDuo.transform(duoDFClusteringBattle)
  val squadPredictedByBattle = models.clusteringByBattleForSquad.transform(squadDFClusteringBattle)
  Array(soloPredictedByBattle, duoPredictedByBattle, squadPredictedByBattle)


  /** Clustering2 --- Clustering by Distance
    *
    * Get an array of the DataFrame with prediction results of clustering by distance, respectively for solo matches, duo matches and squad matches
    */
    val inputCols = Array("player_dist_ride", "player_dist_walk")
    //Get the dateframe with features column
    val soloDFClusteringDist = ClusteringByDistance.createDfWithFeature(soloPlayers, inputCols)
    val duoDFClusteringDist = ClusteringByDistance.createDfWithFeature(duoPlayers, inputCols)
    val squadDFClusteringDist = ClusteringByDistance.createDfWithFeature(squadPlayers, inputCols)
    //Get the prediction result of clustering by distance
    val soloPredictedByDist = models.clusteringByDistForSolo.transform(soloDFClusteringDist)
    val duoPredictedByDist = models.clusteringByDistForDuo.transform(duoDFClusteringDist)
    val squadPredictedByDist = models.clusteringByDistForSquad.transform(squadDFClusteringDist)
    Array(soloPredictedByDist, duoPredictedByDist, squadPredictedByDist)


  /** Use case 2 : Classification
    * Before classification, we need to filter the data by removing the unreasonable data and separate them by party_size
    */
    val filteredDSForClassification = Classification.filterPlayers(dataset)
    val soloMatch = filteredDSForClassification.filter(d => d.party_size == 1).cache()
    val duoMatch = filteredDSForClassification.filter(d => d.party_size == 2).cache()
    val squadMatch = filteredDSForClassification.filter(d => d.party_size == 4).cache()

    val soloDFClassification = Classification.createDfWithFeature(soloMatch)
    val duoDFClassification = Classification.createDfWithFeature(duoMatch)
    val squadDFClassification = Classification.createDfWithFeature(squadMatch)

    val soloClassification = models.nnModelForSolo.transform(soloDFClassification)
    val duoClassification = models.nnModelForDuo.transform(duoDFClassification)
    val squadClassification = models.nnModelForSquad.transform(squadDFClassification)
    Array(soloClassification, duoClassification, squadClassification)

}