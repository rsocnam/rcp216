package cnam


import java.io.FileWriter
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import scala.io.Source

class StreamGenerator(interval: Int = 2, history: Int = 7) extends Thread
{
  protected var finished = false;
  protected var index = 0;
  protected var lines:Array[String] = Array()
  protected val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  protected var limit = 100

  var withHistory:Boolean = false
  var withDate:Boolean = false
  var appendTestData:Boolean = false

  override def run(): Unit = {
    startStream()
  }

  protected def startStream(): Unit = {
    cleanDirectory(Conf.scanTestDir)
    cleanDirectory(Conf.scanTrainDir)

    lines = Source.fromFile(Conf.file)
      .getLines()
      .filter(!_.startsWith("date"))
      .toArray

    while (!finished) {
      streamRow()
      Thread.sleep(interval * 1000)
    }
  }

  protected def streamRow(): Unit = {
    def convertRow(row: String): String = if (this.withDate) "%s,%s\n".format(dateFormat.format(new Date()), row) else "%s\n".format(row)

    var from = index * Conf.metricsPerDay
    val to = from + Conf.metricsPerDay

    val defaultTrainFile = Conf.scanTrainDir + "/" + index.formatted("%06d") + ".txt"
    val defaultTestFile = Conf.scanTestDir + "/" + index.formatted("%06d") + ".txt"

    if (withHistory) {
      from = from - history * Conf.metricsPerDay;
      if (from < 0) {
        from = 0
      }
    }
    finished = index >= limit ||
      0 == lines.slice(to, to + Conf.metricsPerDay).length

    if (!finished) {
      val trainWriter = new FileWriter(defaultTrainFile)

      lines
        .slice(from, to)
        .foreach(str => trainWriter.write(convertRow(str)))

      trainWriter.flush()
      trainWriter.close()

      val testWriter = new FileWriter(if (appendTestData) defaultTrainFile else defaultTestFile, appendTestData)

      lines
        .slice(to, to + Conf.metricsPerDay)
        .foreach(str => testWriter.write(convertRow(str)))

      testWriter.flush()
      testWriter.close()

      index = index + 1
    }
  }

  protected def cleanDirectory(directory: String): Unit = {
    Files.list(Paths.get(directory)).toArray.map(_.toString).foreach(s => {
      Files.delete(Paths.get(s).toRealPath())
    })
  }
}
