
name := "scala_projectone"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++={
  val sparkVer = "2.3.0"
  Seq(
    "org.apache.spark" % "spark-core_2.11" % sparkVer
  )
  Seq(
    "org.apache.spark"%"spark-sql_2.11"% sparkVer
  )
  Seq(
    "org.apache.spark"%"spark-mllib_2.11"%sparkVer
  )

}
libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.9.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.0.0" % "test"