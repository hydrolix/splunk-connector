ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "splunk-interop"
  )

assemblyMergeStrategy := {
  case PathList(pl@_*) if pl.last == "module-info.class" => MergeStrategy.discard
  case PathList(pl@_*) if pl.last == "public-suffix-list.txt" => MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", "native", _*) => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "logback.xml" => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

mainClass := Some("io.hydrolix.splunk.HdxPlanCommand")

libraryDependencies := Seq(
  "io.hydrolix" %% "hydrolix-spark-connector" % "1.1.0-SNAPSHOT",
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
)