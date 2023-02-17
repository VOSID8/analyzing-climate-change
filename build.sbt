lazy val root = project
  .in(file("."))
  .settings(
    name         := "DataV",
    organization := "visionofsid",
    scalaVersion := "2.12.8",
    version      := "0.1.0-SNAPSHOT",
    libraryDependencies += "org.scalafx" % "scalafx_2.12" % "19.0.0-R30",
    libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.3.1",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.3.1",
    libraryDependencies += "org.apache.spark" % "spark-mllib_2.12" % "3.3.1",
	  libraryDependencies += "org.apache.spark" % "spark-graphx_2.12" % "3.3.1"
  )