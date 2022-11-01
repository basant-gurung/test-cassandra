import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "test-cassandra"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.1"

lazy val root = (project in file("."))
  .settings(
    organization := "com.pkware.cassandra",
    version := "0.0.1",
    scalaVersion := "2.11.12",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions := Seq("-target:jvm-1.8"),
    resolvers ++= Seq(
      "Typesafe releases" at "https://repo.typesafe.com/typesafe/releases",
      "apache-snapshots" at "https://repository.apache.org/snapshots/",
      "Hortonworks Repository" at "https://repo.hortonworks.com/content/repositories/releases/"
    ),

    homepage := Some(url("https://www.pkware.com/")),
    description := "This Jar is created to run tests for cassandra connection",

    assembly / assemblyJarName := "test-cassandra-1.0.jar",
    assembly / mainClass := Some("com.pkware.cassandra.Main"),
    assembly / logLevel := Level.Info,
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false).withCacheUnzip(false),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.0",
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2",
      "org.scala-lang" % "scala-library" % "2.11.12"
    ))
