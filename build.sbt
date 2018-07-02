name := "Actor Database"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.12"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
    "org.scalactic" %% "scalactic" % "3.0.5",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "org.apache.commons" % "commons-io" % "1.3.2" % Test
)

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
