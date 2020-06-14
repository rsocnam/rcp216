package cnam.handler

import cnam.Utils.ResultWriter
import cnam.{Conf, StreamGenerator}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamAuto extends Handler
{
  override def run(interval: Int, historyDays: Int): Unit = {
    val conf = new SparkConf().setMaster("local[8]").setAppName("RCP216")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))

    def strConverter(str: String): String = {
      val arr = str.split(",")
      "(%s,[%s, %s, %s, %s, %s])".format(arr(3), arr(1), arr(2), arr(4), arr(6), arr(7))
    }

    val writer = new ResultWriter("spark-stream-%d.csv".format(historyDays))

    val trainData = ssc.textFileStream(Conf.scanTrainDir)
      .map(strConverter)
      .map(LabeledPoint.parse)
      .cache()

    val numFeatures = 5
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))

    val testData = ssc
      .textFileStream(Conf.scanTestDir)
      .map(strConverter)
      .map(LabeledPoint.parse)

    model.trainOn(trainData)
    model.predictOnValues(testData.map(v => (v.label, v.features))).foreachRDD(rdd => {
      if (rdd.count() > 0) {
        writer.save(rdd.map(arr => arr._2).collect(), rdd.map(arr => arr._1).collect())
      } else {
        println("No test data")
      }
    })

    ssc.start()

    val t = new StreamGenerator(interval, history = historyDays)
    t.start()
  }
}
