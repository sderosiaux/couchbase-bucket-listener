package com.ctheu.couchbase.graphstages

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

/**
  * Counts the elements passing through and reset each time it's pulled.
  *
  * It never backpressures.
  */
class DeltaCounter[T] extends GraphStage[FlowShape[T, Long]] {
  override val shape = FlowShape(Inlet[T]("DeltaCounter.in"), Outlet[Long]("DeltaCounter.out"))
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    var counter = 0L
    setHandlers(shape.in, shape.out, this)

    override def preStart(): Unit = tryPull(shape.in)

    override def onPush(): Unit = {
      counter += 1
      tryPull(shape.in)
    }

    override def onPull(): Unit = {
      push(shape.out, counter)
      counter = 0
    }
  }
}
