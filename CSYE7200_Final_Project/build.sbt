name := "CSYE7200_Final_Project"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVer = "2.1.0"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-mllib" % sparkVer,
    "org.scalatest" %% "scalatest" % "3.0.1",
    "co.theasi" %% "plotly" % "0.2.0"
  )
}
