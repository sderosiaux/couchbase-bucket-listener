package com.ctheu.couchbase

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.GraphDSL.Builder
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Source, Zip, ZipWith, ZipWithN}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.ctheu.couchbase.UI.KeyWithCounters
import de.heikoseeberger.akkasse.ServerSentEvent
import play.api.libs.json.Json

import concurrent.duration._
import scala.runtime.Nothing$

object Testing extends App {
  implicit val sys = ActorSystem()
  implicit val mat = ActorMaterializer()

  val src = Source.queue[Long](100, OverflowStrategy.backpressure)

}

final class RepeatLastOrDefault[T](initial: T) extends GraphStage[FlowShape[T, T]] {
  override val shape = FlowShape(Inlet[T]("HoldWithInitial.in"), Outlet[T]("HoldWithInitial.out"))
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var currentValue: T = initial
    setHandlers(shape.in, shape.out, new InHandler with OutHandler {
      override def onPush() = {
        currentValue = grab(shape.in)
        pull(shape.in)
      }
      override def onPull() = {
        push(shape.out, currentValue)
      }
    })

    override def preStart() = pull(shape.in)
  }

}

final class DefaultIfEmpty[T](initial: T) extends GraphStage[FlowShape[T, T]] {
  override val shape = FlowShape(Inlet[T]("HoldWithInitial.in"), Outlet[T]("HoldWithInitial.out"))
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var currentValue: T = initial
    setHandlers(shape.in, shape.out, new InHandler with OutHandler {
      override def onPush() = {
        currentValue = grab(shape.in)
        pull(shape.in)
      }
      override def onPull() = {
        push(shape.out, currentValue)
        currentValue = initial
      }
    })

    override def preStart() = pull(shape.in)
  }

}

object UI {
  case class KeyWithCounters[T](key: T, total: Long, lastDelta: Long)
  case class KeyWithExpiry(key: String, expiry: Long)
  type MutationKey = KeyWithCounters[KeyWithExpiry]
  type SimpleKey = KeyWithCounters[String]
  case class Combinaison(mutations: MutationKey, deletions: SimpleKey, expirations: SimpleKey)

  def withCounters[Out, Mat](source: Source[Out, Mat]): Source[KeyWithCounters[Out], Mat] = {
    Source.fromGraph(GraphDSL.create(source) { implicit b => s =>
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[Out](3))
      val sumPerTick = b.add(Flow[Out].conflateWithSeed(_ => 0L)((r, _) => r + 1))
      val sumTotal = b.add(Flow[Out].scan(0L)((acc, _) => acc + 1).conflate[Long]((_, last) => last))
      val zip = b.add(ZipWith((key: Out, total: Long, last: Long) => KeyWithCounters(key, total, last)))

      def hold[T](init: T) = b.add(Flow.fromGraph(new RepeatLastOrDefault(init)))
      def default[T](init: T) = b.add(Flow.fromGraph(new DefaultIfEmpty(init)))
      def repeat = b.add(Flow[Long].expand(sum => Iterator.continually(sum)))

      s ~> broadcast
           broadcast ~>                              zip.in0
           broadcast ~> sumPerTick ~> default(0L) ~> zip.in1
           broadcast ~> sumTotal   ~> hold(0L)    ~> zip.in2

      SourceShape(zip.out)
    })
  }

  def route()(implicit sys: ActorSystem, mat: Materializer): Route = {
    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkasse.EventStreamMarshalling._

    import concurrent.duration._

    implicit val expiryJson = Json.writes[KeyWithExpiry]
    implicit val keysJson = Json.writes[KeyWithCounters[String]]
    implicit val keysJson2 = Json.writes[KeyWithCounters[KeyWithExpiry]]
    implicit val combinaisonJson2 = Json.writes[Combinaison]

    path("events" / """[a-z0-9\._]+""".r / """[a-z_]+""".r) { (host, bucket) =>
      get {
        complete {
          val (mutations, deletions, expirations) = CouchbaseSource.createSources(host, bucket)
          val sseTick = Source.tick(1 second, 1 second, NotUsed)
          val mutationsWithCounts = withCounters(mutations.expand(_ => Iterator.continually(null.asInstanceOf[KeyWithExpiry])))
          val deletionsWithCounts = withCounters(deletions)
          val expirationsWithCounts = withCounters(expirations)

          val allCounters = GraphDSL.create(mutationsWithCounts, deletionsWithCounts, expirationsWithCounts)((_, _, _)) { implicit b => (m, d, e) =>
            import GraphDSL.Implicits._
            val zip = b.add(ZipWith[MutationKey, SimpleKey, SimpleKey, (MutationKey, SimpleKey, SimpleKey)]((_, _, _)))
            m ~> zip.in0
            d ~> zip.in1
            e ~> zip.in2
            SourceShape(zip.out)
          }

          val allCountersTicked = sseTick.zipWith(allCounters) { case (_, counters) => counters }

            allCountersTicked.map { case (m: MutationKey, d: SimpleKey, e: SimpleKey) =>
              ServerSentEvent(Json.toJson(Combinaison(m, d, e)).toString())
            }
            .keepAlive(1.second, () => ServerSentEvent.heartbeat)
        }
      }
    } ~ path("ui" / """[a-z0-9\._]+""".r / """[a-z_]+""".r) { (host, bucket) =>
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,
          s"""
             |<html>
             |<head>
             |</head>
             |<body>
             |<h1>Bucket: $bucket</h1>
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
            |var source = new EventSource('/events/$host/$bucket');
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
}
