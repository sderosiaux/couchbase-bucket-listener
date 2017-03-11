package com.ctheu.couchbase.graphstages

import akka.NotUsed
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, ZipWith}

case class Accumulator[Out](total: Long, lastDelta: Long, last: Seq[Out])

object Counters {

  /**
    * A Flow that keeps track of some stats and of the last viewed items.
    *
    * It never backpressures.
    */
  def flow[In, Out, Mat](n: Int = 10): Graph[FlowShape[Out, Accumulator[Out]], NotUsed] = {
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[Out](3))
      def sumPerTick = b.add(Flow.fromGraph(new DeltaCounter[Out]()))
      def sumTotal = b.add(Flow.fromGraph(new Counter[Out]()))
      def nLast(n: Int) = b.add(Flow.fromGraph(new NLastDistinctItemsGraphStage[Out](n)))
      val zip = b.add(ZipWith((total: Long, delta: Long, last: Seq[Out]) => Accumulator(total, delta, last)))

      broadcast ~> sumTotal   ~> zip.in0
      broadcast ~> sumPerTick ~> zip.in1
      broadcast ~> nLast(n)   ~> zip.in2

      FlowShape(broadcast.in, zip.out)
    }
  }
}
