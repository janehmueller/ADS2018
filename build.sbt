import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

name := "Actor Database"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.12"
/*
libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % "2.5.5",
    "org.scalactic" %% "scalactic" % "3.0.5",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
)
*/
val `akka-sample-multi-node-scala` = project
    .in(file("."))
    .settings(multiJvmSettings: _*)
    .settings(
        organization := "com.typesafe.akka.samples",
        scalaVersion := "2.12.6",
        libraryDependencies ++= Seq(
            "com.typesafe.akka" %% "akka-actor" % akkaVersion,
            "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
            "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
            "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
            "com.typesafe.akka" %% "akka-multi-node-testkit" % "2.5.5",
            "org.scalactic" %% "scalactic" % "3.0.5",
            "org.scalatest" %% "scalatest" % "3.0.5" % Test),
        // disable parallel tests
        parallelExecution in Test := false,
        licenses := Seq(("CC0", url("http://creativecommons.org/publicdomain/zero/1.0")))
    )
    .configs (MultiJvm)


// testing settings
// disable buffering of logging until all tests for a file are complete
logBuffered in Test := false
// writes test output to stdout with all durations
testOptions in Test := Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oD"))
// JVM options
// increase initial heap size
// increase maximum heap size
// remove unused classes from memory
javaOptions ++= Seq("-Xms512M", "-Xmx4096M", "-XX:+CMSClassUnloadingEnabled")
// explicitly enables forking in tests
fork in Test := true
// disable parallel execution due to file access in tests
parallelExecution in Test := false

// disables testing for assembly
test in assembly := {}

// suppresses include info and merge warnings
logLevel in assembly := Level.Error
