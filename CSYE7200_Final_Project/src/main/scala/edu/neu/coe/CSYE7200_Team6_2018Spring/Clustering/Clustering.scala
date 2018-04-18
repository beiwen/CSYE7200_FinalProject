package edu.neu.coe.CSYE7200_Team6_2018Spring.Clustering

import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player
import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player.IngestPlayer
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Dataset}

trait Clustering {

  def clustering(ds: Dataset[Player]) : Array[KMeansModel] = {
    // filter rows by removing some rows with the obviously unreasonable data.
    val filteredDS = IngestPlayer.filterPlayers(ds)
    // filter data by party_size
    val soloPlayers = filteredDS.filter(d => d.party_size == 1).cache()
    val duoPlayers = filteredDS.filter(d => d.party_size == 2).cache()
    val squadPlayers = filteredDS.filter(d => d.party_size == 4).cache()
    Array(soloPlayers, duoPlayers, squadPlayers).map(pd => clusteringHelper(pd))
  }

  def createDfWithFeature(ds: Dataset[Player],inputCols: Array[String]): DataFrame = {
    val vecAss = new VectorAssembler().setInputCols(inputCols).setOutputCol("unscaled_features")
    val assembledDf = vecAss.transform(ds)
    val scaler = new StandardScaler().setInputCol("unscaled_features").setOutputCol("features")
    val scalerModel = scaler.fit(assembledDf)
    val scaledDf = scalerModel.transform(assembledDf)
    dropCols(scaledDf)
  }

  def clusteringHelper(dsSeprated: Dataset[Player]): KMeansModel

  def dropCols(df : DataFrame): DataFrame

  def determinK(assembledDf: DataFrame): IndexedSeq[(Int, Double)] = {
    val clusters = Range(2, 10)
    clusters zip clusters.map(k => new KMeans().setK(k).setSeed(1L).fit(assembledDf).computeCost(assembledDf))
  }
}
