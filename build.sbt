name := "spark-sample-project"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.0"

resolvers += "Lightbend Repository" at "http://repo.lightbend.com/lightbend/releases/"
resolvers += "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases"
resolvers += "MVN Repo" at "http://mvnrepository.com/artifact"

//spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalanlp" %% "breeze" % "latest.integration",
  "org.scalanlp" %% "breeze-viz" % "0.12"
)

// logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

// testing
libraryDependencies += "org.scalatest"  %% "scalatest" % "2.2.6" % "test"

//config
libraryDependencies += "com.typesafe" % "config" % "1.3.1"


