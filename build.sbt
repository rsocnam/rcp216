ThisBuild / scalaVersion := "2.11.11"

ThisBuild / organization := "net.cnam"

val sparkVersion = "2.4.5"

lazy val app = (project in file("."))
  .settings(
    name := "Project RCP216",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  )
