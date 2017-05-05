package com.ctheu.couchbase

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling._
import akka.stream._
import akka.stream.scaladsl.SourceQueueWithComplete
import com.couchbase.client.java.document._
import com.couchbase.client.java.{AsyncBucket, CouchbaseCluster}
import com.ctheu.couchbase.graphstages.{Deletion, Expiration, Accumulator, Mutation}
import de.heikoseeberger.akkasse.ServerSentEvent
import play.api.libs.json.Json

import scala.concurrent.Promise
import scala.language.postfixOps
import scala.util.{Failure, Success}

object UI {

  def route()(implicit sys: ActorSystem, mat: Materializer): Route = {
    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkasse.EventStreamMarshalling._

    import concurrent.duration._
    implicit val ec = sys.dispatcher

    implicit val muts = Json.writes[Mutation]
    implicit val dels = Json.writes[Deletion]
    implicit val exps = Json.writes[Expiration]
    implicit val accumulatorMut = Json.writes[Accumulator[Mutation]]
    implicit val accumulatorDel = Json.writes[Accumulator[Deletion]]
    implicit val accumulatorExp = Json.writes[Accumulator[Expiration]]
    implicit val accumulators = Json.writes[Accumulators[Mutation, Deletion, Expiration]]

    implicit val durationMarshaller = Unmarshaller.strict[String, FiniteDuration](s => FiniteDuration(s.toInt, "ms"))

    val DEFAULT_DURATION: FiniteDuration = 200 millis
    val DEFAULT_COUNT = 10

    // TODO(sd): add some TTL to close them
    val connectionsCache = collection.mutable.Map[(String, String), AsyncBucket]()

    path("documents" / """[-a-z0-9\._]+""".r / """[-a-z0-9\._]+""".r / """[-a-zA-Z0-9\._:]+""".r) { (host, bucket, key) =>
      get {
        parameters('pwd.as[String] ?) { pwd =>
          // TODO(sd): move into a repository
          val couchbaseGet = {
            import rx.lang.scala.JavaConverters._
            val b = connectionsCache.getOrElseUpdate((host, bucket), CouchbaseCluster.create(host).openBucket(bucket, pwd.getOrElse("")).async())
            b.get(key, classOf[RawJsonDocument]).asScala.map(_.content())
          }
          onComplete(couchbaseGet.toBlocking.toFuture) {
            case Success(res) => complete(HttpEntity(res))
            case Failure(e) => complete(HttpResponse(StatusCodes.InternalServerError, entity = e.getMessage))
          }
        }
      }
    } ~ path("events" / """[-a-z0-9\._]+""".r / """[-a-z0-9_]+""".r) { (host, bucket) =>
      get {
        parameters('interval.as[FiniteDuration] ? DEFAULT_DURATION, 'n.as[Int] ? DEFAULT_COUNT, 'pwd.as[String] ?, 'filter.as[String] ?) { (interval, nLast, pwd, filter) =>
          complete {
            CouchbaseGraph.source(host, bucket, pwd, nLast, interval, filter)
              .map { acc => ServerSentEvent(Json.toJson(acc).toString()) }
              .keepAlive(1 second, () => ServerSentEvent.heartbeat)
          }
        }
      }
    } ~ path("ui" / """[-a-z0-9\._]+""".r) { host =>
      get {
        parameter('user.as[String], 'pwd.as[String]) { (user, pwd) =>
          import collection.JavaConverters._
          val cli = CouchbaseCluster.create(host)
          val buckets = cli.clusterManager(user, pwd).getBuckets.asScala
          cli.disconnect()

          def bucketHref(name: String) =
            s"""
              |<li><a href="/ui/$host/$name">$name</a></li>
            """.stripMargin

          complete(HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            s"""
               |<h1>Buckets available on $host</h1>
               |<ul>
               |${buckets.map(_.name()).map(bucketHref).mkString("")}
               |</ul>
             """.stripMargin
          ))
        }
      }
    } ~ path("ui" / """[-a-z0-9\._]+""".r / """[-a-z0-9_]+""".r) { (host, bucket) =>
      get {
        // TODO(sd): I tried to forget all my front-end knowledge. Time to add some React and Scala.js?
        parameter('interval.as[Int] ? DEFAULT_DURATION, 'n.as[Int] ? DEFAULT_COUNT, 'pwd.as[String] ?, 'filter.as[String] ?) { (interval, n, pwd, filter) =>
          complete(HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            s"""
               |<html>
               |<head>
               |<style>
               |.type { display: flex; }
               |#panels { display: flex; }
               |.type div { display: flex; align-items: center; justify-content: center; padding: 10px; flex-direction: column; }
               |.type span { font-weight: bold; font-size: 20px; }
               |#right { flex: 1; margin: 10px; padding: 20px; background: rgba(0,0,0,.1); border: 1px solid #ccc; }
               |</style>
               |<link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/highlight.js/9.10.0/styles/idea.min.css">
               |</head>
               |<body>
               |<h1>Bucket: $bucket</h1>
               |<div id="panels">
               |<div id="left">
               |<h3>Mutations</h3>
               |<div class="type"><canvas id="mutation" width="400" height="100"></canvas><div>Total<span id="muttotal"></span></div></div>
               |<h3>Deletions</h3>
               |<div class="type"><canvas id="deletion" width="400" height="100"></canvas><div>Total<span id="deltotal"></span></div></div>
               |<!--<h3>Expirations</h3>
               |<div class="type"><canvas id="expiration" width="400" height="100"></canvas><div>Total<span id="exptotal"></span></div></div>-->
               |<h3>Last $n documents mutated (key, expiry), earliest to oldest</h3>
               |<pre class="prettyprint" id="lastMutation"></pre>
               |</div>
               |<pre id="right">Click on a mutated document key to display its content here!
               |</pre>
               |</div>
               |<script src="//cdnjs.cloudflare.com/ajax/libs/highlight.js/9.10.0/highlight.min.js"></script>               |
               |<script src="//cdnjs.cloudflare.com/ajax/libs/smoothie/1.27.0/smoothie.min.js"></script>
               |<script>
               |function ch(id) {
               |const chart = new SmoothieChart({millisPerPixel:100,minValue:0,maxValueScale:1.5,grid:{strokeStyle:'rgba(119,119,119,0.43)'}});
               |const series = new TimeSeries();
               |chart.addTimeSeries(series, { strokeStyle: 'rgba(0, 255, 0, 1)', fillStyle: 'rgba(0, 255, 0, 0.2)', lineWidth: 1 });
               |chart.streamTo(document.getElementById(id), 500);
               |return series;
               |}
               |const mut = ch("mutation")
               |const del = ch("deletion")
               |//const exp = ch("expiration")
               |
               |function update(sel, value) { document.getElementById(sel + "total").innerHTML = value; }
               |function show(key) {
               |  const block = document.getElementById("right")
               |  document.getElementById("right").innerHTML = "Fetching " + key + " ..."
               |  fetch('/documents/$host/$bucket/' + key + '?pwd=${pwd.getOrElse("")}')
               |    .then(res => {
               |      if (res.status == 500) return res.text()
               |      else { return res.json() }
               |    })
               |    .then(doc => {
               |      block.innerHTML = JSON.stringify(doc, null, 2)
               |      hljs.highlightBlock(block)
               |      block.innerHTML = key + "\\n" + "-".repeat(key.length) + "\\n\\n\\n" + block.innerHTML
               |    })
               |}
               |var source = new EventSource('/events/$host/$bucket?interval=${interval.toMillis}&n=$n${pwd.map("&pwd=" + _).getOrElse("")}${filter.map("&filter=" + _).getOrElse("")}');
               |source.addEventListener('message', function(e) {
               |  var data = JSON.parse(e.data);
               |  mut.append(new Date().getTime(), data.mutations.lastDelta); update("mut", data.mutations.total);
               |  del.append(new Date().getTime(), data.deletions.lastDelta); update("del", data.deletions.total);
               |  //exp.append(new Date().getTime(), data.expirations.lastDelta); update("exp", data.expirations.total);
               |  document.getElementById("lastMutation").innerHTML = data.mutations.last.reverse().reduce((acc, x) => acc + "<a href=\\"javascript: show('" + x.key + "')\\">" + x.key + "</a> (" + (x.expiry > 0 ? new Date(x.expiry*1000).toISOString() : "0") + ")" + "<br>", "");
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
}
