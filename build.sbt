import ScaldingPlugin.Dependency._

name := "scalding-avro-example"

organization := "com.danosipov"

scalaVersion := "2.11.6"

version := "1.0"

// Enable Scalding plugin
enablePlugins(ScaldingPlugin)

// Include SbtAvro settings, this is the preferred way to enable the plugin
Seq(sbtavro.SbtAvro.avroSettings: _*)

// Upgrade Avro version to the latest available
version in avroConfig := "1.7.7"

// Scalding Plugin doesn't bring in the Avro subproject by default, so we have to include it manually
libraryDependencies ++= Seq(
  scalding_avro
)