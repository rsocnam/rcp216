package cnam.handler

import cnam.Utils.ResultWriter
import cnam.{Conf, StreamGenerator}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredStream extends Handler
{
  override def run(interval: Int, historyDays: Int): Unit = {
    val conf = new SparkConf().setMaster("local[8]").setAppName("RCP216")
    val sc = SparkSession.builder().config(conf).getOrCreate()
    import sc.implicits._

    val schema = new StructType()
      .add("time", TimestampType)
      .add("date", FloatType)
      .add("day", IntegerType)
      .add("period", FloatType)
      .add("nsw_price", FloatType)
      .add("nsw_demand", FloatType)
      .add("vic_price", FloatType)
      .add("vic_demand", FloatType)
      .add("transfer", FloatType)
      .add("label", StringType)

    val df: DataFrame = sc.readStream
      .schema(schema)
      .format("csv")
      .option("delimiter", ",")
      .load(Conf.scanTrainDir)

    case class Metric(day:Int, period: Float, nsw_price: Float, nsw_demand: Float, vic_demand: Float, transfer: Float)

    val writer = new ResultWriter("spark-struct-stream-rf-%d.csv".format(historyDays))

    df
      .withWatermark("time", "%d seconds".format(interval))
      .select("day", "period", "nsw_price", "nsw_demand", "vic_demand", "transfer")
      .writeStream
      .outputMode("update")
      .foreachBatch((batch, index) => {
        val seq = batch.collect()
          .map(row => (row.getInt(0), row.getFloat(1), row.getFloat(2), row.getFloat(3), row.getFloat(4), row.getFloat(5)))
          .toSeq

        val trainSize = batch.count().toInt - Conf.metricsPerDay
        if (trainSize > 0) {

          val assembler = new VectorAssembler()
            .setInputCols(Array("day", "period", "nsw_demand", "vic_demand", "transfer"))
            .setOutputCol("features")

          val dfTrain = assembler
            .transform(seq.take(trainSize)
              .toDF("day", "period", "nsw_price", "nsw_demand", "vic_demand", "transfer")
            )
            .select("nsw_price", "features")
            .withColumnRenamed("nsw_price", "label")
            .repartition(2)
            .cache()

          val dfTest = assembler.transform(seq.takeRight(Conf.metricsPerDay)
            .toDF("day", "period", "nsw_price", "nsw_demand", "vic_demand", "transfer")
          )
            .select("nsw_price", "features")
            .withColumnRenamed("nsw_price", "label")

          val lr = new LinearRegression()
            .setMaxIter(50)
            .setRegParam(0.1)

          val rf = new RandomForestRegressor()
          val gbt = new GBTRegressor()

          val paramGrid = new ParamGridBuilder()
//            .addGrid(lr.regParam, Array(.1, .05, .01))
            .addGrid(rf.numTrees, Array(10, 15, 30, 50))
//            .addGrid(gbt.maxIter, Array(10, 15, 30))
            .build()

          val evaluator = new RegressionEvaluator().setMetricName("rmse")

          val cv = new CrossValidator()
//            .setEstimator(lr)
            .setEstimator(rf)
            .setEvaluator(evaluator)
            .setEstimatorParamMaps(paramGrid)
            .setNumFolds(8)
            .setParallelism(4)

          val cvModel = cv.fit(dfTrain)

          val predictions = cvModel.transform(dfTest)
            .select("label", "prediction").collect()

          writer.save(
            predictions.map(row => row.getFloat(0).toDouble),
            predictions.map(row => row.getDouble(1))
          )
        } else {
          println("Skipped for first day")
        }
      })
      .start()

    println("Start generation")
    val t = new StreamGenerator(interval = interval, history = historyDays)
    t.withDate = true
    t.withHistory = true
    t.appendTestData = true
    t.start()
  }
}
