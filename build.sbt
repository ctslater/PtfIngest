name := "Ptf Ingest"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"
libraryDependencies += "com.thesamet" %% "kdtree" % "1.0.4"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.163"


