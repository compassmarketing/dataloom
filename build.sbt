lazy val commonSettings = Seq(
  organization := "Dataloom",
  name := "dataloom",
  version := "0.1",
  scalaVersion := "2.11.8",
)
lazy val shaded = (project in file("."))
  .settings(commonSettings)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2"  % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.3.2"  % "provided",
  //"com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.apache.commons" % "commons-text" % "1.1",
  "com.github.scopt" % "scopt_2.11" % "3.6.0"
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
)

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(shaded).settings(
  scalaVersion := "2.11.8",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.2" % "compile",
    "org.apache.spark" %% "spark-sql" % "2.3.2"  % "compile",
    "org.apache.spark" %% "spark-mllib" % "2.3.2"  % "compile"
  )
)