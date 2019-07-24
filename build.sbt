import com.typesafe.config.{ConfigFactory, Config}

lazy val appProperties = settingKey[Config]("The application properties")
lazy val fileproperties=ConfigFactory.parseFile(new File("src/main/resources/application.conf"))
lazy val properties=ConfigFactory.load(fileproperties)
lazy val sparkVersion=properties.getString("spark.spark-sql-version")
lazy val AppName=properties.getString("scala.name")
lazy val AppVersion = properties.getString("scala.version")
lazy val AppscalaVersion = properties.getString("scala.scalaVersion")

name:= AppName
version := AppVersion
scalaVersion := AppscalaVersion


resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.1"
libraryDependencies += "net.virtual-void" %%  "json-lenses" % "0.6.2"





test in assembly := {}

