package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler

object Clustering {


  def clusteringByDistModel(ds: Dataset[Player]): KMeansModel = {

    def createDfWithFeature(ds: Dataset[Player]): DataFrame = {
      val vecAss = new VectorAssembler().setInputCols(Array("player_dist_ride", "player_dist_walk")).setOutputCol("features")
      val output = vecAss.transform(ds)
      return output
    }

    val kmeans = new KMeans().setK(3).setSeed(1L)
    val fitDf = createDfWithFeature(ds)
    val model = kmeans.fit(fitDf)
    return model
  }

  def filtInactivePlayers(ds: Dataset[Player]): Dataset[Player] = {
    val activePlayers = ds.filter(d => (d.player_dist_ride != 0 || d.player_dist_walk != 0))
    return activePlayers
  }

}
