package com.ctheu.couchbase

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.LongAdder

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.{Client => _, _}
import akka.stream.scaladsl.{GraphDSL, Keep, Merge, RunnableGraph, Sink, Source, SourceQueueWithComplete}
import akka.stream.stage._
import com.couchbase.client.dcp._
import com.couchbase.client.dcp.config.DcpControl
import com.couchbase.client.dcp.message.{DcpDeletionMessage, DcpExpirationMessage, DcpMutationMessage, DcpSnapshotMarkerRequest}
import de.heikoseeberger.akkasse.ServerSentEvent

import collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.parsing.json.JSON


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

    class CounterGraphStage extends GraphStageWithMaterializedValue[SourceShape[Long], LongAdder] {
      override val shape = SourceShape(Outlet[Long]("counter.out"))

      override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, LongAdder) = {
        val counter = new LongAdder
        (new GraphStageLogic(shape) {
          setHandler(shape.out, new OutHandler {
            override def onPull() = {
              push(shape.out, counter.longValue())
              counter.increment()
            }
          })
        }, counter)
      }
    }

  class NLastDistinctItemsGraphStage[T](n: Int) extends GraphStageWithMaterializedValue[SinkShape[T], mutable.Queue[T]] {
    override val shape = SinkShape(Inlet[T]("nLastItems.in"))

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, mutable.Queue[T]) = {
      val queue = new mutable.Queue[T]
      (new GraphStageLogic(shape) {

        setHandler(shape.in, new InHandler {
          override def onPush(): Unit = {
            val e = grab(shape.in)
            if (!queue.contains(e)) {
              queue += e
              if (queue.size > n) queue.dequeue()
            }
            tryPull(shape.in)
          }
        })

        override def preStart(): Unit = {
          tryPull(shape.in)
        }
      }, queue)
    }
  }

//  class CounterGraphStage[T] extends GraphStageWithMaterializedValue[SinkShape[T], LongAdder] {
//    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, LongAdder) = {
//      val counter = new LongAdder()
//      (
//        new GraphStageLogic(shape) {
//
//          this.setHandler(shape.in, new InHandler {
//            override def onPush() = {
//              grab(shape.in) // ignore
//              counter.increment()
//              tryPull(shape.in)
//            }
//          })
//
//          override def preStart(): Unit = {
//            tryPull(shape.in)
//          }
//        }, counter)
//    }
//
//    override val shape: SinkShape[T] = SinkShape(Inlet("counter.in"))
//  }


  implicit val system = ActorSystem("couchbase-listener")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  case class MatValue[T](queue: SourceQueueWithComplete[T], count: LongAdder, lastItems: mutable.Queue[T])
  case class Counters[T, U, V](mutations: MatValue[T], deletions: MatValue[U], expirations: MatValue[V])

  def listenTo(bucket: String): Counters[(String, Int), String, String] = {
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

    def newGraph[T] = Source.queue[T](1000, OverflowStrategy.dropHead)
                         .zipWithMat(Source.fromGraph(new CounterGraphStage))(Keep.left)(Keep.both)
                         .toMat(Sink.fromGraph(new NLastDistinctItemsGraphStage(10))) { case ((a, b), c) => MatValue(a, b, c) }

    val mutations = newGraph[(String, Int)].run()
    val deletions = newGraph[String].run()
    val expirations = newGraph[String].run()

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
        println("unknown")
      }
      event.release()
    }

    client.connect().await()
    client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await()
    client.startStreaming().await()

    Counters(mutations, deletions, expirations)
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
                           |  "mutation": ${stats.mutations.count.longValue()},
                           |  "deletion": ${stats.deletions.count.longValue()},
                           |  "expiration": ${stats.expirations.count.longValue()},
                           |  "lastDocsMutated": [${stats.mutations.lastItems.mkString("\"","\",\"","\"")}]
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
            |<h1>Bucket: ${bucket}</h1>
            |<h3>Mutations</h3>
            |<canvas id="mutation" width="400" height="100"></canvas>
            |<h3>Deletions</h3>
            |<canvas id="deletion" width="400" height="100"></canvas>
            |<h3>Expirations</h3>
            |<canvas id="expiration" width="400" height="100"></canvas>
            |<h3>Last 10 documents mutated (key, expiry)</h3>
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
            |  document.getElementById("lastMutation").innerHTML = data.lastDocsMutated.reduce((acc, x) => acc + x + "<br>", "");
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
