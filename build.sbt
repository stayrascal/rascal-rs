name := "recommendation"

parallelExecution in Test := false
test in assembly := {}

scalaVersion := "2.11.8"

val mahoutVersion = "0.13.0"

libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.12.1" % "provided",
  "org.apache.predictionio" %% "apache-predictionio-data" % "0.12.1" % "provided",
  "org.apache.predictionio" %% "apache-predictionio-data-elasticsearch" % "0.12.1" % "provided",
  "org.apache.predictionio" %% "apache-predictionio-data-elasticsearch1" % "0.11.0-incubating" % "provided",
  "com.mashape.unirest" % "unirest-java" % "1.4.9" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.1.1" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.apache.mahout" %% "mahout-math-scala" % mahoutVersion,
  "org.apache.mahout" %% "mahout-spark" % mahoutVersion
    exclude("org.apache.spark", "spark-core_2.11"),
  "org.apache.mahout" % "mahout-math" % mahoutVersion,
  "org.apache.mahout" % "mahout-hdfs" % mahoutVersion
    exclude("com.thoughtworks.xstream", "xstream")
    exclude("org.apache.hadoop", "hadoop-client"),
  "org.elasticsearch" %% "elasticsearch-spark" % "2.4.5"
    exclude("org.apache.spark", "spark-catalyst_2.11")
    exclude("org.apache.spark", "spark-sql_2.11")
)
