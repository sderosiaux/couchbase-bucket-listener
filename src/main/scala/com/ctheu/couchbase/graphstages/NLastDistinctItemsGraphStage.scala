package com.ctheu.couchbase.graphstages

import akka.stream.stage._
import akka.stream._

import scala.collection.mutable

/**
  * Keep the N last elements passing through.
  *
  * It never backpressures.
  */
class NLastDistinctItemsGraphStage[T](n: Int) extends GraphStage[FlowShape[T, Seq[T]]] {
  override val shape = FlowShape(Inlet[T]("NLastDistinctItemsGraphStage.in"), Outlet[Seq[T]]("NLastDistinctItemsGraphStage.out"))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val queue = new mutable.Queue[T]

    new GraphStageLogic(shape) with InHandler with OutHandler {

      setHandlers(shape.in, shape.out, this)

      override def preStart(): Unit = {
        tryPull(shape.in)
      }

      override def onPush(): Unit = {
        val e = grab(shape.in)
        if (!queue.contains(e)) {
          queue += e
          if (queue.size > n) queue.dequeue()
        }
        tryPull(shape.in)
      }

      override def onPull(): Unit = {
        push(shape.out, queue)
      }
    }
  }
}
