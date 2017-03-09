package com.ctheu.couchbase

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Source, SourceQueueWithComplete, ZipWith}
import com.ctheu.couchbase.graphstages.{Counter, DeltaCounter}

import scala.concurrent.duration._

object CouchbaseGraph {

  type SimpleKey = KeyWithCounters
  case class KeyWithCounters(total: Long, lastDelta: Long)
  case class Combinaison(mutations: SimpleKey, deletions: SimpleKey, expirations: SimpleKey)

  def withCounters[Out, Mat](source: Source[Out, Mat]): Source[KeyWithCounters, Mat] = {
    Source.fromGraph(GraphDSL.create(source) { implicit b => s =>
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[Out](2))
      def sumPerTick = b.add(Flow.fromGraph(new DeltaCounter[Out]()))
      def sumTotal = b.add(Flow.fromGraph(new Counter[Out]()))
      val zip = b.add(ZipWith((total: Long, last: Long) => KeyWithCounters(total, last)))

      s ~> broadcast
      broadcast ~> sumTotal   ~> zip.in0
      broadcast ~> sumPerTick ~> zip.in1

      SourceShape(zip.out)
    })
  }

  def counters(host: String, bucket: String, interval: FiniteDuration) = {
    val (mutations, deletions, expirations) = CouchbaseSource.createSources()
    val mutationsWithCounts = withCounters(mutations)
    val deletionsWithCounts = withCounters(deletions)
    val expirationsWithCounts = withCounters(expirations)

    val allCounters = GraphDSL.create(mutationsWithCounts, deletionsWithCounts, expirationsWithCounts)((_, _, _)) { implicit b =>
      (m, d, e) =>
        import GraphDSL.Implicits._
        val zip = b.add(ZipWith[SimpleKey, SimpleKey, SimpleKey, (SimpleKey, SimpleKey, SimpleKey)]((_, _, _)))
        m ~> zip.in0
        d ~> zip.in1
        e ~> zip.in2
        SourceShape(zip.out)
    }

    Source.tick(1 second, interval, NotUsed)
          .zipWithMat(allCounters)(Keep.right)(Keep.right)
  }
}
