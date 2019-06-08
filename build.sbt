
name := "spark_lab"

organization := "au.com.me.nig"

version := "0.1"

scalaVersion := "2.11.12"

val avroVersion = "1.8.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0" % Provided

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.8"

libraryDependencies += "org.apache.spark" %% "spark-tags" % "2.4.0"

libraryDependencies += "org.apache.avro" % "avro" % avroVersion
libraryDependencies += "org.apache.avro" % "avro-mapred" % avroVersion
libraryDependencies += "org.apache.avro" % "avro-ipc" % avroVersion


dependencyOverrides += "net.minidev" % "json-smart" % "2.3"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "com.typesafe" % "config" % "1.3.3"

dependencyOverrides += "org.apache.hadoop" % "hadoop-client" % "2.8.5"

libraryDependencies ++= Seq(
  ("org.apache.hadoop" % "hadoop-aws" % "2.8.5").
    exclude("javax.servlet", "servlet-api").
    exclude("javax.servlet.jsp", "jsp-api").
    exclude("commons-collections", "commons-collections").
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("com.sun.jersey", "jersey-server")
)


libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.12"
