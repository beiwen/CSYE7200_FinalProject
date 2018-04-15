package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.udf

object Classification {
  def buildingModel(ds: Dataset[Player]): Unit = {
    def buildModelHelper(ds: Dataset[Player]): Unit = {
      val dataFrame = createDfWithFeature(ds).cache()
      val splits = dataFrame.randomSplit(Array(0.7, 0.3), seed = 1234L)
      val trian =splits(0)
      val test = splits(1)
    }
  }

  def createDfWithFeature(ds: Dataset[Player]): DataFrame ={
    val isWinnerUdf = udf((placement: Int) => placement match {
      case 1 => 1
      case _ => 0
    })
    val colArray = Array("player_assists", "player_dbno", "player_dist_ride", "player_dist_walk", "player_dmg", "player_kills")
    val vecAss = new VectorAssembler().setInputCols(colArray).setOutputCol("feature_unnormalized")
    val normalizer = new Normalizer().setInputCol("feature_unnormalized").setOutputCol("features")
    val assembledDf = vecAss.transform(normalizer.transform(ds))
    val resultDf = assembledDf.withColumn("survived", isWinnerUdf(assembledDf("team_placement")))
    resultDf
  }

  def filtPlayers(ds: Dataset[Player]): Dataset[Player] = {
    //We need to filter abnormal data. Some players have "0" as team_placement, which do not affect clustering but obviously affect classification.
    val filterdPlayers = ds.filter(d => (d.player_dist_ride != 0 || d.player_dist_walk != 0)
      && d.player_survive_time <= 2400
      && d.team_placement > 0)
    filterdPlayers
  }
}
