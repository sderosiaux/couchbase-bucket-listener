package com.ctheu.couchbase

import java.util.concurrent.atomic.LongAdder

import akka.stream._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import scala.collection.mutable


class CounterGraphStage extends GraphStageWithMaterializedValue[SourceShape[Long], LongAdder] {
  override val shape = SourceShape(Outlet[Long]("counter.out"))

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, LongAdder) = {
    val counter = new LongAdder
    (new GraphStageLogic(shape) {
      setHandler(shape.out, new OutHandler {
        override def onPull() = {
          push(shape.out, counter.longValue())
          counter.increment()
        }
      })
    }, counter)
  }
}
