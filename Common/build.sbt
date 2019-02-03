name := "Common"

version := "1.0"

organization := "br.ufrn.dimap.forall.spark"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "com.google.code.gson" % "gson" % "2.8.0",
  "com.groupon.sparklint" %% "sparklint-spark210" % "1.0.12"
)
