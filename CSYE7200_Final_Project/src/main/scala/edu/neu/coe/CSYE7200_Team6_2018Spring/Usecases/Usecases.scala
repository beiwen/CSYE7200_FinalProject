package edu.neu.coe.CSYE7200_Team6_2018Spring.Usecases

import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player
import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player.IngestPlayer
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

object Usecases {

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

  val schema: StructType = Player.schema
  implicit val spark: SparkSession = Player.spark
  val dataset: Dataset[Player] = IngestPlayer.ingest("sample.csv", schema)

}