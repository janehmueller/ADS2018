name := "Actor Database"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.12"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion
)
