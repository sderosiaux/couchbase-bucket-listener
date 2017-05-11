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
          val buckets = cli.clusterManager(user, pwd).getBuckets.asScala.map(_.name()).toList
          cli.disconnect()

          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, templates.html.buckets.render(host, buckets).toString()))
        }
      }
    } ~ path("ui" / """[-a-z0-9\._]+""".r / """[-a-z0-9_]+""".r) { (host, bucket) =>
      get {
        parameter('interval.as[Int] ? DEFAULT_DURATION, 'n.as[Int] ? DEFAULT_COUNT, 'pwd.as[String] ?, 'filter.as[String] ?) { (interval, n, pwd, filter) =>
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, templates.html.ui.render(host, bucket, interval, n, pwd, filter).toString()))
        }
      }
    }
  }
}
