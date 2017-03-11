package com.ctheu.couchbase

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Source, ZipWith}
import com.ctheu.couchbase.graphstages.{Counter, DeltaCounter, NLastDistinctItemsGraphStage}

import scala.concurrent.duration._

object CouchbaseGraph {

  case class KeyWithCounters[Out](total: Long, lastDelta: Long, last: Seq[Out])
  case class Combinaison[Out1, Out2, Out3](mutations: KeyWithCounters[Out1], deletions: KeyWithCounters[Out2], expirations: KeyWithCounters[Out3])

  def withCounters[Out, Mat](source: Source[Out, Mat], n: Int = 10): Source[KeyWithCounters[Out], Mat] = {
    Source.fromGraph(GraphDSL.create(source) { implicit b => s =>
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[Out](3))
      def sumPerTick = b.add(Flow.fromGraph(new DeltaCounter[Out]()))
      def sumTotal = b.add(Flow.fromGraph(new Counter[Out]()))
      def nLast(n: Int) = b.add(Flow.fromGraph(new NLastDistinctItemsGraphStage[Out](n)))
      val zip = b.add(ZipWith((total: Long, delta: Long, last: Seq[Out]) => KeyWithCounters(total, delta, last)))

      s ~> broadcast
      broadcast ~> sumTotal   ~> zip.in0
      broadcast ~> sumPerTick ~> zip.in1
      broadcast ~> nLast(n)  ~> zip.in2

      SourceShape(zip.out)
    })
  }

  def source(host: String, bucket: String, interval: FiniteDuration, nLast: Int = 10) = {
    // TODO(sd): create a proper GraphStage[SourceShape[T, U]] to act as source? Without relying on a Queue
    val (mutations, deletions, expirations) = CouchbaseSource.createSources()
    val mutationsWithCounts = withCounters(mutations, nLast)
    val deletionsWithCounts = withCounters(deletions, nLast)
    val expirationsWithCounts = withCounters(expirations, nLast)

    val allCounters = GraphDSL.create(mutationsWithCounts, deletionsWithCounts, expirationsWithCounts)((_, _, _)) { implicit b =>
      (m, d, e) =>
        import GraphDSL.Implicits._
        val zip = b.add(ZipWith[KeyWithCounters[KeyWithExpiry], KeyWithCounters[String], KeyWithCounters[String], (KeyWithCounters[KeyWithExpiry], KeyWithCounters[String], KeyWithCounters[String])]((_, _, _)))
        m ~> zip.in0
        d ~> zip.in1
        e ~> zip.in2
        SourceShape(zip.out)
    }

    Source.tick(1 second, interval, NotUsed)
          .zipWithMat(allCounters)(Keep.right)(Keep.right)
  }
}
