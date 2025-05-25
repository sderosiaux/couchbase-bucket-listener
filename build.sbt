import play.twirl.sbt.Import.TwirlKeys.compileTemplates

name := "couchbase-listener"

version := "1.0"

scalaVersion := "2.12.18"

resolvers += Resolver.mavenLocal

libraryDependencies += "com.couchbase.client" % "dcp-client" % "0.10.0" // Reverting to original version
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.20"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.36"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36"
// libraryDependencies += "de.heikoseeberger" %% "akka-sse" % "X.Y.Z" // This line (or its variants) is to be removed
libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.2.10" // Added Akka HTTP
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.4"
libraryDependencies += "com.couchbase.client" % "java-client" % "2.7.15" // Trying another common 2.x version
libraryDependencies += "io.reactivex" %% "rxscala" % "0.27.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-testkit" % "10.2.10" % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.6.20" % Test,
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

enablePlugins(JavaAppPackaging)
enablePlugins(SbtTwirl)

(Compile / TwirlKeys.compileTemplates / sourceDirectories) := (Compile / unmanagedResourceDirectories).value
