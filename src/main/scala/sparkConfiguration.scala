import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait sparkConfiguration {

  val config:Config=ConfigFactory.load("Cassandra.conf")
  val connection_host =config.getString("connection_host")
  val cassandra_host =config.getString("cassandra_host")
 // val spk=config.getString("spark")
  val name=config.getString("AppName")
  // spark Master



  val spark = SparkSession.builder
    .appName(name)
    .config(connection_host,cassandra_host)
    .master("local")
    .getOrCreate()

  val conf = new SparkConf(true)
    .set(connection_host,cassandra_host)
    .setAppName(name)
    .setMaster("local")


}
