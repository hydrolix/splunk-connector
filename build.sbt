ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

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

mainClass := Some("io.hydrolix.splunk.HdxQueryCommand")

//enablePlugins(SbtProguard)
//
//Proguard / proguardOptions ++= Seq("-dontnote", "-ignorewarnings")
//(Proguard / proguard) / javaOptions := Seq("-Xmx2g")
//
//Proguard / proguardOptions += ProguardOptions.keepMain(mainClass.value.get)

libraryDependencies ++= Seq(
  "io.hydrolix" %% "hydrolix-spark-connector" % "1.1.1-SNAPSHOT",
  ("org.apache.spark" %% "spark-sql" % "3.4.1").exclude("org.apache.logging.log4j", "log4j-slf4j2-impl"),
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "org.apache.curator" % "curator-recipes" % "5.5.0",
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
)
