ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "splunk-interop"
  )

javacOptions := Seq("-source", "11", "-target", "11")

assemblyMergeStrategy := {
  case PathList(pl@_*) if pl.last == "module-info.class" => MergeStrategy.discard
  case PathList(pl@_*) if pl.last == "public-suffix-list.txt" => MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", "native", _*) => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "logback.xml" => MergeStrategy.first
  case PathList("org", "apache", "commons", "logging", _*) => MergeStrategy.first
  case PathList("google", "protobuf", "any.proto" | "empty.proto" | "descriptor.proto") => MergeStrategy.first
  case "arrow-git.properties" => MergeStrategy.first
  case "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat" => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

mainClass := Some("io.hydrolix.connectors.splunk.HdxQueryCommand")

libraryDependencies ++= Seq(
  "io.hydrolix" %% "hydrolix-connectors-core" % "1.5.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "org.apache.curator" % "curator-recipes" % "5.5.0",
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
)
