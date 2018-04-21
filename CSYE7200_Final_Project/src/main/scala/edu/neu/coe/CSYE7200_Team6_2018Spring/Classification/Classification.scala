package edu.neu.coe.CSYE7200_Team6_2018Spring.Classification

import edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest.Player
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset}

object Classification {
  def buildingNNModels(ds: Dataset[Player]): Array[MultilayerPerceptronClassificationModel] = {
    def buildModelHelper(ds: Dataset[Player]): MultilayerPerceptronClassificationModel = {
      val dataFrame = createDfWithFeature(ds).cache()
      val splits = dataFrame.randomSplit(Array(0.7, 0.3), seed = 1L)
      val trian =splits(0)
      val test = splits(1)
      //The layer number and node size of NN was determined according to StackOverflow answers: https://stats.stackexchange.com/questions/181/how-to-choose-the-number-of-hidden-layers-and-nodes-in-a-feedforward-neural-netw?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
      val layers = Array(7, 5, 2)
      val trainer = new MultilayerPerceptronClassifier()
          .setLayers(layers)
          .setLabelCol("survived")
          .setMaxIter(300)
          .setSeed(1234L)

      val model = trainer.fit(trian)
      val result = model.transform(test)
      val predictionLabels = result.select("prediction", "survived")
      val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy").setLabelCol("survived")
      println("Test set accuracy = " + evaluator.evaluate(predictionLabels))
      model
    }

    val soloPlayers = ds.filter(d => d.party_size == 1).cache()
    val duoPlayers = ds.filter(d => d.party_size == 2).cache()
    val squadPlayers = ds.filter(d => d.party_size == 4).cache()

    Array(soloPlayers, duoPlayers, squadPlayers).map(pd => buildModelHelper(pd))

  }

  def buildingRFModels(ds: Dataset[Player]): Array[RandomForestClassificationModel] = {
    def buildModelHelper(ds: Dataset[Player]): RandomForestClassificationModel = {
      val dataFrame = createDfWithFeature(ds).cache()
      val splits = dataFrame.randomSplit(Array(0.7, 0.3), seed = 1L)
      val trian =splits(0)
      val test = splits(1)

      val trainer = new RandomForestClassifier()
        .setLabelCol("survived")
        .setNumTrees(100)
        .setSeed(1234L)

      val model = trainer.fit(trian)
      val result = model.transform(test)
      val predictionLabels = result.select("prediction", "survived")
      val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy").setLabelCol("survived")
      println("Test set accuracy = " + evaluator.evaluate(predictionLabels))
      model
    }

    val soloPlayers = ds.filter(d => d.party_size == 1).cache()
    val duoPlayers = ds.filter(d => d.party_size == 2).cache()
    val squadPlayers = ds.filter(d => d.party_size == 4).cache()

    val models = Array(soloPlayers, duoPlayers, squadPlayers).map(pd => buildModelHelper(pd))
    models
  }
  // We also tried LR Models, unfortunately, we did not find a appropriate parameter set.
  def buildingLRModels(ds: Dataset[Player]): Array[LogisticRegressionModel] = {
    def buildModelHelper(ds: Dataset[Player]): LogisticRegressionModel = {
      val dataFrame = createDfWithFeature(ds).cache()
      val splits = dataFrame.randomSplit(Array(0.7, 0.3), seed = 1L)
      val trian =splits(0)
      val test = splits(1)

      val trainer = new LogisticRegression()
        .setLabelCol("survived")

      val model = trainer.fit(trian)
      val result = model.transform(test)
      val predictionLabels = result.select("prediction", "survived")
      val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy").setLabelCol("survived")
      println("Test set accuracy = " + evaluator.evaluate(predictionLabels))
      model
    }

    val soloPlayers = ds.filter(d => d.party_size == 1).cache()
    val duoPlayers = ds.filter(d => d.party_size == 2).cache()
    val squadPlayers = ds.filter(d => d.party_size == 4).cache()

    val models = Array(soloPlayers, duoPlayers, squadPlayers).map(pd => buildModelHelper(pd))
    models
  }

  def createDfWithFeature(ds: Dataset[Player]): DataFrame ={
    val isWinnerUdf = udf((placement: Int) => placement match {
      case 1 => 1
      case _ => 0
    })
    val colArray = Array("player_assists", "player_dbno", "player_dist_ride", "player_dist_walk", "player_dmg", "player_kills", "player_survive_time")
    val vecAss = new VectorAssembler().setInputCols(colArray).setOutputCol("feature_unnormalized")
    val normalizer = new Normalizer().setInputCol("feature_unnormalized").setOutputCol("features")
    val df_temp_1 = vecAss.transform(ds)
    val df_temp_2 = normalizer.transform(df_temp_1)
    val resultDf = df_temp_2.withColumn("survived", isWinnerUdf(df_temp_2("team_placement")))
    resultDf
  }

  def filterPlayers(ds: Dataset[Player]): Dataset[Player] = {
    //We need to filter abnormal data. Some players have "0" as team_placement, which do not affect clustering but obviously affect classification.
    val filterdPlayers = ds.filter(d => (d.player_dist_ride != 0 || d.player_dist_walk != 0)
      && d.player_survive_time <= 2400
      && d.team_placement > 0)
    filterdPlayers
  }
}