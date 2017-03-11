package com.ctheu.couchbase.graphstages

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.couchbase.client.dcp.config.DcpControl
import com.couchbase.client.dcp.message.{DcpDeletionMessage, DcpExpirationMessage, DcpMutationMessage, DcpSnapshotMarkerRequest}
import com.couchbase.client.dcp.{Client, StreamFrom, StreamTo}

sealed trait CouchbaseEvent
case class Mutation(key: String, expiry: Int) extends CouchbaseEvent
case class Deletion(key: String) extends CouchbaseEvent
case class Expiration(key: String) extends CouchbaseEvent

object CouchbaseSource {
  def DCPClient(hostname: String, bucket: String) = {
    Client.configure()
      .bucket(bucket)
      .hostnames(hostname)
      .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 10000) // set the buffer to 10K
      .bufferAckWatermark(75) // after 75% are reached of the 10KB, acknowledge against the serv
      .build()
  }
}

class CouchbaseSource(hostname: String, bucket: String) extends GraphStage[SourceShape[CouchbaseEvent]] {

  override val shape = SourceShape(Outlet[CouchbaseEvent]("CouchbaseSource.out"))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    import CouchbaseSource._

    new GraphStageLogic(shape) with OutHandler with StageLogging {

      var client: Client = _

      setHandler(shape.out, this)

      val cb = getAsyncCallback[CouchbaseEvent] { event =>
        if (isAvailable(shape.out)) {
          push(shape.out, event)
        }
        // if the port is not available, the event is lost.
        // this is why the next stage should never backpressure, to not lose any bits.
        // we could maintain some buffer and "emit" but I don't like that.
      }

      override def postStop(): Unit = {
        log.info(s"Stage has finished, disconnecting DCP client ($hostname:$bucket) ...")
        client.disconnect()
      }

      override def onDownstreamFinish(): Unit = {
        log.info(s"Downstream finished ($hostname:$bucket)")
        super.onDownstreamFinish()
      }

      override def preStart(): Unit = {
        client = DCPClient(hostname, bucket)
        log.info(s"Connected to Couchbase DCP on $hostname:$bucket")

        bindEventHandlers(client)

        client.connect().await()
        client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await()
        client.startStreaming().await()
        log.info(s"Streaming on $hostname:$bucket has started")

      }

      override def onPull(): Unit = {
        // you will get data when I'll get them!
      }

      def bindEventHandlers(client: Client): Unit = {

        client.controlEventHandler { event =>
          // Those events MUST be acknowledged
          if (DcpSnapshotMarkerRequest.is(event)) {
            client.acknowledgeBuffer(event)
          }
          event.release()
        }

        client.dataEventHandler { event =>
          if (DcpMutationMessage.is(event)) {
            cb.invoke(Mutation(DcpMutationMessage.keyString(event), DcpMutationMessage.expiry(event)))
            client.acknowledgeBuffer(event)
          }
          else if (DcpDeletionMessage.is(event)) {
            cb.invoke(Deletion(DcpDeletionMessage.keyString(event)))
            client.acknowledgeBuffer(event)
          }
          else if (DcpExpirationMessage.is(event)) {
            cb.invoke(Expiration(DcpExpirationMessage.keyString(event)))
            client.acknowledgeBuffer(event)
          }
          else {
            log.warning("Unknown Couchbase event")
          }
          event.release()
        }

      }

    }
  }
}
