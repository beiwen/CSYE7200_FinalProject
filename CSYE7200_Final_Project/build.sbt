name := "CSYE7200_Final_Project"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= {
  val sparkVer = "2.1.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer
  )
}