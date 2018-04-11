package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}

object Clustering {


  def clusteringByDist(ds: Dataset[Player]): KMeansModel = {

    def createDfWithFeature(ds: Dataset[Player]): DataFrame = {
      val vecAss = new VectorAssembler().setInputCols(Array("player_dist_ride", "player_dist_walk")).setOutputCol("unscaled_features")
      val assembledDf = vecAss.transform(ds)

      return assembledDf
    }

    def determinK(assembledDf: DataFrame): Int = {
      val scaler = new StandardScaler().setInputCol("unscaled_features").setOutputCol("features")
      val scalerModel = scaler.fit(assembledDf)
      val scaledDf = scalerModel.transform(assembledDf)

      val clusters = Range(2, 10)
      def wssseList = {
        clusters zip clusters.map(k => new KMeans().setK(k).setSeed(1L).fit(assembledDf).computeCost(assembledDf))
      }
    }

    val k = determinK()

    val kmeans = new KMeans().setK(k).setSeed(1L)
    val fitDf = createDfWithFeature(ds)
    val model = kmeans.fit(fitDf)
    return model
  }

  def filtPlayers(ds: Dataset[Player]): Dataset[Player] = {
    val filtedPlayers = ds.filter(d => (d.player_dist_ride != 0 || d.player_dist_walk != 0) && d.player_survive_time <= 2400)
    return filtedPlayers
  }



}
