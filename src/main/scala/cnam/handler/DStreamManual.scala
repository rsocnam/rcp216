package cnam.handler

import cnam.Utils.ResultWriter
import cnam.{Conf, StreamGenerator}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamManual extends Handler
{
  override def run(interval: Int, historyDays: Int): Unit = {
    val conf = new SparkConf().setMaster("local[8]").setAppName("RCP216")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))

    val lines = ssc.textFileStream(Conf.scanTrainDir)
      .map(_.split(","))

    val windowed = lines.window(Seconds(interval * historyDays), Seconds(interval))

    def convert(arr: Array[String]): LabeledPoint = {
      LabeledPoint.parse("(%s, [%s, %s, %s, %s, %s])".format(arr(3), arr(1), arr(2), arr(4), arr(6), arr(7)))
    }

    val writer = new ResultWriter("manual-stream-%d.csv".format(historyDays))

    windowed.foreachRDD(rdd => {
      val arr = rdd.collect().toSeq
      val rddTrain = spark.sparkContext.parallelize(arr.slice(0, rdd.count().toInt - Conf.metricsPerDay)).map(convert).repartition(16).cache()
      val rddTest = spark.sparkContext.parallelize(arr.slice(rdd.count().toInt - Conf.metricsPerDay, rdd.count().toInt).map(convert))

      if (rddTrain.count() > 0) {
        val model = LinearRegressionWithSGD.train(rddTrain, 50, .1, 1.0)

        val accuracies = model
          .predict(rddTest.map(point => point.features))

        writer.save(rddTest.map(p => p.label).collect(), accuracies.collect())
      } else {
        println("Skip batch")
      }
    })
    ssc.start()

    val t = new StreamGenerator(interval, historyDays)
    t.appendTestData = true
    t.withHistory = true
    t.start()
  }
}
