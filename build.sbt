ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "esgi.iabd"

val sparkVersion = "3.5.0"

val sparkJavaOptions = Seq(
  "-Xmx4g",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

lazy val commonSettings = Seq(
  fork           := true,
  javaOptions  ++= sparkJavaOptions,
  outputStrategy := Some(StdoutOutput)
)

lazy val producer = (project in file("producer"))
  .settings(
    name := "producer",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "com.typesafe"      % "config"     % "1.4.3"
    )
  )

lazy val consumer = (project in file("consumer"))
  .settings(
    name := "consumer",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "com.typesafe"      % "config"     % "1.4.3"
    )
  )

lazy val root = (project in file("."))
  .aggregate(producer, consumer)
  .settings(name := "spark-streaming-images")
