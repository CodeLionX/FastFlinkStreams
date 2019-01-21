ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "fastflinkstreams"

version := "0.0.1"

organization := "com.github.codelionx"

ThisBuild / scalaVersion := "2.12.7"

val flinkVersion = "1.7.1"
val clistVersion = "3.5.0"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"
    ),
    libraryDependencies ++= Seq(
      // logging
      "org.apache.logging.log4j" % "log4j-slf4j18-impl" % "2.11.1",
      "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.11.1",
      // cli parser
      "org.backuity.clist" %% "clist-core" % clistVersion,
      "org.backuity.clist" %% "clist-macros" % clistVersion % "provided",
    )

)

assembly / mainClass := Some("com.github.codelionx.AnalyzeHTTPLog")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
