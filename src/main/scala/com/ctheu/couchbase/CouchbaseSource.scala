package com.ctheu.couchbase

import java.util.concurrent.atomic.LongAdder

import akka.actor.ActorSystem
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import com.couchbase.client.dcp.{Client, StreamFrom, StreamTo}
import com.couchbase.client.dcp.config.DcpControl
import com.couchbase.client.dcp.message.{DcpDeletionMessage, DcpExpirationMessage, DcpMutationMessage, DcpSnapshotMarkerRequest}

import scala.collection.mutable

case class MatValue[T](queue: SourceQueueWithComplete[T], count: LongAdder, lastItems: mutable.Queue[T])
case class RealTimeStats[T, U, V](mutations: MatValue[T], deletions: MatValue[U], expirations: MatValue[V])

object CouchbaseSource {

  def listenTo(hostname: String, bucket: String)(implicit sys: ActorSystem, mat: Materializer): RealTimeStats[(String, Int), String, String] = {

    sys.log.info(s"Will listen to DCP of $hostname:$bucket")

    // Akka Streams, here we go!

    def newGraph[T] = Source.queue[T](1000, OverflowStrategy.dropHead)
      .zipWithMat(Source.fromGraph(new CounterGraphStage))(Keep.left)(Keep.both)
      .toMat(Sink.fromGraph(new NLastDistinctItemsGraphStage(10))) { case ((a, b), c) => MatValue(a, b, c) }

    val mutations = newGraph[(String, Int)].run()
    val deletions = newGraph[String].run()
    val expirations = newGraph[String].run()

    // Populate the sources with Couchbase data callbacks

    val client = createClient(hostname, bucket)

    client.controlEventHandler { event =>
      if (DcpSnapshotMarkerRequest.is(event)) {
        client.acknowledgeBuffer(event)
      }
      event.release()
    }

    client.dataEventHandler { event =>
      if (DcpMutationMessage.is(event)) {
        mutations.queue.offer((DcpMutationMessage.keyString(event), DcpMutationMessage.expiry(event)))
        client.acknowledgeBuffer(event)
      }
      else if (DcpDeletionMessage.is(event)) {
        deletions.queue.offer(DcpDeletionMessage.keyString(event))
        client.acknowledgeBuffer(event)
      }
      else if (DcpExpirationMessage.is(event)) {
        expirations.queue.offer(DcpExpirationMessage.keyString(event))
        client.acknowledgeBuffer(event)
      }
      else {
        sys.log.warning("Unknown Couchbase event")
      }
      event.release()
    }

    // Start the Couchbase streaming

    client.connect().await()
    client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await()
    client.startStreaming().await()

    sys.log.info("Streaming has started")

    // This will contain up-to-date data

    RealTimeStats(mutations, deletions, expirations)
  }

  private def createClient(hostname: String, bucket: String) = {
    Client.configure()
      .bucket(bucket)
      .hostnames(hostname)
      .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 10000) // set the buffer to 10K
      .bufferAckWatermark(75) // after 75% are reached of the 10KB, acknowledge against the serv
      .build()
  }
}
