package com.ctheu.couchbase

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import com.couchbase.client.dcp.config.DcpControl
import com.couchbase.client.dcp.message.{DcpDeletionMessage, DcpExpirationMessage, DcpMutationMessage, DcpSnapshotMarkerRequest}
import com.couchbase.client.dcp.{Client, StreamFrom, StreamTo}

case class KeyWithExpiry(key: String, expiry: Int)

object CouchbaseSource {
  def fill(hostname: String, bucket: String, m: SourceQueueWithComplete[KeyWithExpiry], d: SourceQueueWithComplete[String], e: SourceQueueWithComplete[String])(implicit sys: ActorSystem) = {
    sys.log.info(s"Will listen to DCP of $hostname:$bucket")

    val client = createClient(hostname, bucket)

    client.controlEventHandler { event =>
      if (DcpSnapshotMarkerRequest.is(event)) {
        client.acknowledgeBuffer(event)
      }
      event.release()
    }

    client.dataEventHandler { event =>
      if (DcpMutationMessage.is(event)) {
        m.offer(KeyWithExpiry(DcpMutationMessage.keyString(event), DcpMutationMessage.expiry(event)))
        client.acknowledgeBuffer(event)
      }
      else if (DcpDeletionMessage.is(event)) {
        d.offer(DcpDeletionMessage.keyString(event))
        client.acknowledgeBuffer(event)
      }
      else if (DcpExpirationMessage.is(event)) {
        e.offer(DcpExpirationMessage.keyString(event))
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
  }


  def createSources() = {
    val mutations = Source.queue[KeyWithExpiry](10000, OverflowStrategy.dropHead)
    val deletions = Source.queue[String](10000, OverflowStrategy.dropHead)
    val expirations = Source.queue[String](10000, OverflowStrategy.dropHead)
    (mutations, deletions, expirations)
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
