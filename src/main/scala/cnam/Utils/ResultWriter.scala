package cnam.Utils

import java.io.FileWriter

import cnam.Conf

class ResultWriter(fileName:String) {
  protected var writer:FileWriter = new FileWriter(Conf.resultDir + fileName)

  def save(labels: Array[Double], predictions: Array[Double]): Unit = {
    val mcos = labels.zip(predictions).map(t => Math.pow(t._1 - t._2, 2))
    val meanMcos = mcos.sum / mcos.length

    writer.write("%f;%f\n".format(meanMcos, Math.log10(meanMcos)))
    writer.flush()
  }

  def end(): Unit = {
    writer.close()
  }
}
