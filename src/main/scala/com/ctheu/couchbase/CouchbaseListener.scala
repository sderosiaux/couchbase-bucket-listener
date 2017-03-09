package com.ctheu.couchbase

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.{IncomingConnection, ServerBinding}
import akka.stream._
import akka.stream.scaladsl.Source

import scala.concurrent.Future

object CouchbaseListener extends App {
  implicit val system = ActorSystem("couchbase-listener")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  Http().bindAndHandle(UI.route(), "0.0.0.0", 8080)
        .foreach(binding ⇒ system.log.info(s"HTTP listening on ${binding.localAddress}"))
}
