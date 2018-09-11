import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

name := "Actor_Database"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.12"

//libraryDependencies ++= Seq(
//    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
//    "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
//    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
//    "com.typesafe.akka" %% "akka-multi-node-testkit" % "2.5.5",
//    "org.scalactic" %% "scalactic" % "3.0.5",
//    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
//)

val `actor-database` = project
    .in(file("."))
    .settings(multiJvmSettings: _*)
    .settings(
        organization := "de.hpi.ads",
        scalaVersion := "2.12.6",
        libraryDependencies ++= Seq(
            "com.typesafe.akka" %% "akka-actor" % akkaVersion,
            "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
            "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
            "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
            "org.scalactic" %% "scalactic" % "3.0.5",
            "org.scalatest" %% "scalatest" % "3.0.5"),
        // disable parallel tests
        parallelExecution in Test := false,
        licenses := Seq(("CC0", url("http://creativecommons.org/publicdomain/zero/1.0")))
    ).configs(MultiJvm)

// multi node testing
// pairs of user@host where the tests are executed
multiNodeHosts in MultiJvm := Seq("seminar1807@thor01", "seminar1807@thor02", "seminar1807@thor03", "seminar1807@thor04")
// where the sbt-assembly jar file will be copied to
multiNodeTargetDirName in MultiJvm := "sommer_ehmueller"

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
