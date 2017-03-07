package com.ctheu.couchbase

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.LongAdder

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.couchbase.client.dcp._
import com.couchbase.client.dcp.config.DcpControl
import com.couchbase.client.dcp.message.{DcpDeletionMessage, DcpExpirationMessage, DcpMutationMessage, DcpSnapshotMarkerRequest}
import de.heikoseeberger.akkasse.ServerSentEvent
import collection.JavaConverters._

object CouchbaseListener extends App {

//  def conductor() = {
//    val group = new NioEventLoopGroup
//    val env = ClientEnvironment.builder
//      .setClusterAt(List("couchbase01.stg.ps").asJava)
//      .setConnectionNameGenerator(DefaultConnectionNameGenerator.INSTANCE)
//      .setBucket("budget")
//      .setDcpControl(new DcpControl)
//      .setEventLoopGroup(group, true)
//      .build
//
//    val ctor = new Conductor(env, new HttpStreamingConfigProvider(env))
//    ctor.connect().await()
//
//    Thread.sleep(1000)
//    ctor.stop().andThen(env.shutdown())
//  }


  implicit val system = ActorSystem("couchbase-listener")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher


  case class Stats(
                    mutation: LongAdder = new LongAdder,
                    deletion: LongAdder = new LongAdder,
                    expiration: LongAdder = new LongAdder,
                    lastDocsMutated: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
                  )

  def listenTo(bucket: String) = {
    val client = Client.configure()
      .bucket(bucket)
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

    val stats = Stats()

    client.dataEventHandler { event =>
      if (DcpMutationMessage.is(event)) {
        println("mutation: " + DcpMutationMessage.toString(event))
        stats.mutation.increment()
        stats.lastDocsMutated.add(DcpMutationMessage.keyString(event))
        client.acknowledgeBuffer(event)
        if (stats.lastDocsMutated.size > 10)
          stats.lastDocsMutated.poll()
      }
      else if (DcpDeletionMessage.is(event)) {
        println("deletion: " + DcpMutationMessage.toString(event))
        client.acknowledgeBuffer(event)
        stats.deletion.increment()
      }
      else if (DcpExpirationMessage.is(event)) {
        println("expiration: " + DcpMutationMessage.toString(event))
        client.acknowledgeBuffer(event)
        stats.expiration.increment()
      }
      else {
        println("unknown")
      }
      event.release()
    }

    client.connect().await()
    client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await()
    client.startStreaming().await()

    stats
  }


  def route = {
    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkasse.EventStreamMarshalling._

    import concurrent.duration._

    path("events" / """[a-z_]+""".r) { bucket =>
      get {
        complete {
          val stats = listenTo(bucket)

          Source.tick(1 second, 1 second, NotUsed)
            .map(_ => ServerSentEvent(s"""
                           | {
                           |  "mutation": ${stats.mutation.longValue()},
                           |  "deletion": ${stats.deletion.longValue()},
                           |  "expiration": ${stats.expiration.longValue()},
                           |  "lastDocsMutated": [${stats.lastDocsMutated.asScala.mkString("\"",",","\"")}]
                           | }
                            """.stripMargin))
            .keepAlive(1.second, () => ServerSentEvent.heartbeat)
        }
      }
    } ~ path("ui" / """[a-z_]+""".r) { bucket =>
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,
          s"""
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
            |var source = new EventSource('/events/$bucket');
            |source.addEventListener('message', function(e) {
            |  var data = JSON.parse(e.data);
            |  mut.append(new Date().getTime(), data.mutation);
            |  del.append(new Date().getTime(), data.deletion);
            |  exp.append(new Date().getTime(), data.expiration);
            |  document.getElementById("lastMutation").innerHTML = data.lastDocsMutated.map(x => x + "<br>");
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

  Http().bindAndHandle(route, "0.0.0.0", 8080)

}
