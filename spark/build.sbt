// Package Information

name := "mlstream-spark-udfs" // change to project name
organization := "mlstream" // change to your org
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.12" //"2.12.9" //need to use the same version

// Spark Information
val sparkVersion = "2.4.4"

// allows us to include spark packages
//resolvers += "bintray-spark-packages" at
//  "https://dl.bintray.com/spark-packages/maven/"

//resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Repo".at("https://repo.typesafe.com/typesafe/releases/")

resolvers += "MavenRepository".at("https://mvnrepository.com/")

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // testing
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "test"
)

parallelExecution in Test := false


// Compiler settings. Use scalac -X for other options and their description.
// See Here for more info http://www.scala-lang.org/files/archive/nightly/docs/manual/html/scalac.html
scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint", "-Xfatal-warnings")

// ScalaTest settings.
// Ignore tests tagged as @Slow (they should be picked only by integration test)
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow", "-u", "target/junit-xml-reports", "-oD", "-eS")
