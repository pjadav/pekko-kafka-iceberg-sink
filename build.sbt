import Dependencies.deps
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.{MergeStrategy, PathList}
import sbtbuildinfo.BuildInfoKey
import sbtbuildinfo.BuildInfoKeys.buildInfoKeys

ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "io.github.prakashjadav"
ThisBuild / organizationName := "Prakash Jadav"

lazy val root = (project in file("."))
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](organization, moduleName, name, version, scalaVersion, sbtVersion),
    name := "pekko-connectors-iceberg",
    libraryDependencies ++= deps,
    resolvers += "Confluent" at "https://packages.confluent.io/maven/",
    scalacOptions ++= Seq("-encoding", "utf8", "-deprecation", "-unchecked", "-feature"),
    assembly / assemblyJarName := "pekko-connectors-iceberg.jar",
    Global / lintUnusedKeysOnLoad := false,
    Test / parallelExecution := false,
    Compile / run / fork := true,
    scalafmtOnCompile := true,
    libraryDependencySchemes ++= Seq(
      "com.github.luben" % "zstd-jni" % VersionScheme.Always,
      "com.google.protobuf" % "protobuf-java" % VersionScheme.Always
    ),
    evictionErrorLevel := Level.Warn
  )
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging)

lazy val assemblySettings = assembly / assemblyMergeStrategy := {
  case "module-info.class" => MergeStrategy.discard
  case "reference.conf"    => MergeStrategy.concat
  case "version.conf"      => MergeStrategy.concat
  case PathList("META-INF", "services", _ @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
  case _                                        => MergeStrategy.first
}
