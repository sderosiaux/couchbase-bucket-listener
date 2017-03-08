package com.ctheu.couchbase

import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}

import scala.collection.mutable

class NLastDistinctItemsGraphStage[T](n: Int) extends GraphStageWithMaterializedValue[SinkShape[T], mutable.Queue[T]] {
  override val shape = SinkShape(Inlet[T]("nLastItems.in"))

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, mutable.Queue[T]) = {
    val queue = new mutable.Queue[T]
    (new GraphStageLogic(shape) {

      setHandler(shape.in, new InHandler {
        override def onPush(): Unit = {
          val e = grab(shape.in)
          if (!queue.contains(e)) {
            queue += e
            if (queue.size > n) queue.dequeue()
          }
          tryPull(shape.in)
        }
      })

      override def preStart(): Unit = {
        tryPull(shape.in)
      }
    }, queue)
  }
}
