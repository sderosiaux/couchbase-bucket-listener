package com.ctheu.couchbase

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import de.heikoseeberger.akkasse.ServerSentEvent

object UI {

  def route()(implicit sys: ActorSystem, mat: Materializer): Route = {
    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkasse.EventStreamMarshalling._

    import concurrent.duration._

    path("events" / """[a-z0-9\._]+""".r / """[a-z_]+""".r) { (host, bucket) =>
      get {
        complete {
          val stats = CouchbaseSource.listenTo(host, bucket)

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
