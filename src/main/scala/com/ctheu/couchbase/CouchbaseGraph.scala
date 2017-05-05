package com.ctheu.couchbase

import akka.NotUsed
import akka.stream.ThrottleMode.Shaping
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Partition, Source, ZipWith}
import com.ctheu.couchbase.graphstages._

import scala.concurrent.duration._

case class Accumulators[Out1, Out2, Out3](mutations: Accumulator[Out1], deletions: Accumulator[Out2], expirations: Accumulator[Out3])

object CouchbaseGraph {

  // This source never backpressures
  def accumulatorsSource(host: String, bucket: String, pwd: Option[String], nLast: Int) = {
    Source.fromGraph(GraphDSL.create(Source.fromGraph(new CouchbaseSource(host, bucket, pwd))) { implicit b => src =>
      import GraphDSL.Implicits._

      val p = b.add(Partition[CouchbaseEvent](3, {
        case Mutation(key, expiry) => 0
        case Deletion(key) => 1
        case Expiration(key) => 2
      }))
      val z = b.add(ZipWith((mut: Accumulator[Mutation], del: Accumulator[Deletion], exp: Accumulator[Expiration]) => Accumulators(mut, del, exp)))
      def count[T] = Flow[CouchbaseEvent].map(_.asInstanceOf[T]).via(Counters.flow[T, T, NotUsed](nLast))

      src ~> p
      p.out(0) ~> count[Mutation] ~> z.in0
      p.out(1) ~> count[Deletion] ~> z.in1
      p.out(2) ~> count[Expiration] ~> z.in2

      SourceShape(z.out)
    })
  }

  def source(host: String, bucket: String, pwd: Option[String], nLast: Int = 10, interval: FiniteDuration): Source[Accumulators[Mutation, Deletion, Expiration], NotUsed] = {
    accumulatorsSource(host, bucket, pwd, nLast).throttle(1, interval, 1, Shaping)
  }
}
