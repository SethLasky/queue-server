ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "distributed-server",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "toolkit" % "0.1.17",
      "org.typelevel" %% "toolkit-test" % "0.1.17" % Test,
      "org.http4s" %% "http4s-ember-server" % "0.23.23",
      "org.http4s" %% "http4s-dsl" % "0.23.23",
    )
  )
