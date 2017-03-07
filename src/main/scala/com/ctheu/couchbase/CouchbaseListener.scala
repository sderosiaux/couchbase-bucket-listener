package com.ctheu.couchbase

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, ConcurrentSkipListSet}
import java.util.concurrent.atomic.LongAdder

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.couchbase.client.dcp.conductor.{Conductor, ConfigProvider, HttpStreamingConfigProvider}
import com.couchbase.client.dcp.config.{ClientEnvironment, DcpControl}
import com.couchbase.client.dcp.message.{DcpDeletionMessage, DcpExpirationMessage, DcpMutationMessage, DcpSnapshotMarkerRequest}
import com.couchbase.client.dcp.state.StateFormat
import com.couchbase.client.dcp._
import com.couchbase.client.deps.io.netty.buffer.ByteBuf
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup
import de.heikoseeberger.akkasse.ServerSentEvent
import rx.Completable

import collection.JavaConverters._
import scala.collection.mutable

object CouchbaseListener extends App {

  def conductor() = {
    val group = new NioEventLoopGroup
    val env = ClientEnvironment.builder
      .setClusterAt(List("couchbase01.stg.ps").asJava)
      .setConnectionNameGenerator(DefaultConnectionNameGenerator.INSTANCE)
      .setBucket("budget")
      .setDcpControl(new DcpControl)
      .setEventLoopGroup(group, true)
      .build

    val ctor = new Conductor(env, new HttpStreamingConfigProvider(env))
    ctor.connect().await()

    Thread.sleep(1000)
    ctor.stop().andThen(env.shutdown())
  }

  def run() = {
    val client = Client.configure()
      .bucket("budget")
      .hostnames("couchbase01.stg.ps")
      .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 10000) // set the buffer to 10K
      .bufferAckWatermark(75) // after 75% are reached of the 10KB, acknowledge against the serv
      .build()

    client.controlEventHandler { event =>
      if (DcpSnapshotMarkerRequest.is(event)) {
        client.acknowledgeBuffer(event)
      }
      event.release()
    }

    val mutation = new LongAdder
    val deletion = new LongAdder
    val expiration = new LongAdder
    var lastDocsMutated = new ConcurrentLinkedQueue[String]()

    client.dataEventHandler { event =>
      if (DcpMutationMessage.is(event)) {
        println("mutation: " + DcpMutationMessage.toString(event))
        mutation.increment()
        lastDocsMutated.add(DcpMutationMessage.keyString(event))
        if (lastDocsMutated.size > 10)
          lastDocsMutated.poll()
      }
      else if (DcpDeletionMessage.is(event)) {
        println("deletion: " + DcpMutationMessage.toString(event))
        deletion.increment()
      }
      else if (DcpExpirationMessage.is(event)) {
        println("expiration: " + DcpMutationMessage.toString(event))
        expiration.increment()
      }
      else {
        println("unknown")
      }
      event.release()
    }

    client.connect().await()
    client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await()
    client.startStreaming().await()


    implicit val system = ActorSystem("couchbase-listener")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    def route = {
      import akka.http.scaladsl.server.Directives._
      import de.heikoseeberger.akkasse.EventStreamMarshalling._
      import concurrent.duration._

      path("events") {
        get {
          complete {
            Source.tick(1 second, 1 second, NotUsed)
              .map(_ => ServerSentEvent(s"""
                             | {
                             |  "mutation": ${mutation.longValue()},
                             |  "deletion": ${deletion.longValue()},
                             |  "expiration": ${expiration.longValue()},
                             |  "lastDocsMutated": [${lastDocsMutated.asScala.mkString("\"",",","\"")}]
                             | }
                              """.stripMargin))
              .keepAlive(1.second, () => ServerSentEvent.heartbeat)
          }
        }
      } ~ path("ui") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,
            """
              |<html>
              |<head>
              |</head>
              |<body>
              |<h3>Mutations</h3>
              |<canvas id="mutation" width="400" height="100"></canvas>
              |<h3>Deletions</h3>
              |<canvas id="deletion" width="400" height="100"></canvas>
              |<h3>Expirations</h3>
              |<canvas id="expiration" width="400" height="100"></canvas>
              |<h3>Last documentation mutated</h3>
              |<pre id="lastMutation"></pre>
              |</div>
              |<script src="//cdnjs.cloudflare.com/ajax/libs/smoothie/1.27.0/smoothie.min.js"></script>
              |<script>
              |function ch(id) {
              |const chart = new SmoothieChart({millisPerPixel:100,maxValueScale:1.5,grid:{strokeStyle:'rgba(119,119,119,0.43)'}});
              |const series = new TimeSeries();
              |chart.addTimeSeries(series, { strokeStyle: 'rgba(0, 255, 0, 1)', fillStyle: 'rgba(0, 255, 0, 0.2)', lineWidth: 4 });
              |chart.streamTo(document.getElementById(id), 1000);
              |return series;
              |}
              |const mut = ch("mutation")
              |const del = ch("deletion")
              |const exp = ch("expiration")
              |
              |var source = new EventSource('events');
              |source.addEventListener('message', function(e) {
              |  var data = JSON.parse(e.data);
              |  mut.append(new Date().getTime(), data.mutation);
              |  del.append(new Date().getTime(), data.deletion);
              |  exp.append(new Date().getTime(), data.expiration);
              |  document.getElementById("lastMutation").innerHTML = data.lastDocsMutated;
              |}, false);
              |</script>
              |</body>
              |</html>
              |
            """.stripMargin
          ))
        }
      }
    }

    Http().bindAndHandle(route, "localhost", 8080)
  }


  run()

}
