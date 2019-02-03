name := "AMPLab-Big-Data-Benchmark-Experiments"

version := "1.0"

organization := "br.ufrn.dimap.forall.spark"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

lazy val Common = RootProject(file("../Common"))

lazy val main = Project(id="AMPLab-Big-Data-Benchmark-Experiments", base=file("."))
  .dependsOn(
    Common
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
)
