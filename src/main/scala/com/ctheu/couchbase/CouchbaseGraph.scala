package com.ctheu.couchbase

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Source, SourceQueueWithComplete, ZipWith}
import com.ctheu.couchbase.graphstages.{Counter, DeltaCounter, NLastDistinctItemsGraphStage}

import scala.concurrent.duration._

object CouchbaseGraph {

  type SimpleKey[Out] = KeyWithCounters[Out]
  case class KeyWithCounters[Out](total: Long, lastDelta: Long, last: Seq[Out])
  case class Combinaison[Out](mutations: SimpleKey[Out], deletions: SimpleKey[Out], expirations: SimpleKey[Out])

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
    val (mutations, deletions, expirations) = CouchbaseSource.createSources()
    val mutationsWithCounts = withCounters(mutations, nLast)
    val deletionsWithCounts = withCounters(deletions, nLast)
    val expirationsWithCounts = withCounters(expirations, nLast)

    val allCounters = GraphDSL.create(mutationsWithCounts, deletionsWithCounts, expirationsWithCounts)((_, _, _)) { implicit b =>
      (m, d, e) =>
        import GraphDSL.Implicits._
        val zip = b.add(ZipWith[SimpleKey[String], SimpleKey[String], SimpleKey[String], (SimpleKey[String], SimpleKey[String], SimpleKey[String])]((_, _, _)))
        m ~> zip.in0
        d ~> zip.in1
        e ~> zip.in2
        SourceShape(zip.out)
    }

    Source.tick(1 second, interval, NotUsed)
          .zipWithMat(allCounters)(Keep.right)(Keep.right)
  }
}
