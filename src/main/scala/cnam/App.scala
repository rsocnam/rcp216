package cnam

import cnam.handler.{DStreamAuto, DStreamManual, Handler, StructuredStream}

object App extends App {
  try {
    val interval: Int = args(0).toInt
    val history: Int = args(1).toInt
    val method: String = args(2)

    val handlers: Map[String, Handler] = Map(
      ("auto", DStreamAuto),
      ("manual", DStreamManual),
      ("structured", StructuredStream)
    )

    try {
      handlers(method).run(interval, history)
    } catch {
      case e: NoSuchElementException => {
        println("Unknown method. Available methods: %s".format(handlers.keys.mkString(", ")))
      }
    }

  } catch {
    case e: NumberFormatException => {
      println("interval & history arguments should be integers")
    }
    case e: ArrayIndexOutOfBoundsException => {
      println("Missing arguments. Arguments should be interval, history, method")
    }
  }
}
