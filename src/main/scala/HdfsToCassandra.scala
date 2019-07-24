
import com.datastax.spark.connector._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import resultcheck._
object HdfsToCassandra  extends App with sparkConfiguration {

        val cassandrafuntion = new cassandraFunctions
        val csvfile: Config = ConfigFactory.load("Cassandra.conf")
        val dfFilter = readCsv(csvfile.getString("csvFile")).dropDuplicates("Code_commune_INSEE")
        val dfFilterCode = dfFilter.filter(dfFilter("Code_postal").rlike("90100"))
        initifile()
        writeIncassandra()
        checkresult()

        def writeIncassandra()
        {
             // println("nombre de ligne traiter par Spark avant le filtre: " + dfFilter.count())
              //println("nombre de ligne filtrer par spark : " + dfFilterCode.count())
              cassandrafuntion.createtable("test", "hdfs")
              cassandrafuntion.createtable("test", "hdfsfilter")
              dfFilter.rdd.saveToCassandra("test", "hdfs", SomeColumns("code", "nom", "codepostale", "libelle", "ligne5", "gps"))
              dfFilterCode.rdd.saveToCassandra("test", "hdfsfilter", SomeColumns("code", "nom", "codepostale", "libelle", "ligne5", "gps"))

        }

      def readCsv(path:String) : DataFrame =
      {
             val df = spark.read.format("com.databricks.spark.csv")
                                .option("delimiter", ";")
                                .option("header", "true")
                                .option("inferSchema", "true")
                                .load(path)
              return df
        }

      def checkresult(): Unit =
      {
              import spark.implicits._
               var c=""
                val rddRead = spark.sparkContext.cassandraTable("test", "hdfs")
                val rddFilterRead = spark.sparkContext.cassandraTable("test", "hdfsfilter")
               // println("nombre de ligne inserer avant le filtre: " + rddRead.count())
                //println("nombre de ligne inserer apres le filtre: " + rddFilterRead.count())


                if (rddRead.count()==dfFilter.count())
                {
                   c = generate("passed").toString
                }
                  else if(rddRead.count()!=dfFilter.count()) {
                  c = generate("failed").toString
                }
                else {
                  c = generate("bloqued").toString
                }

                val caseClassDs=Seq(c).toDS()
                              caseClassDs.show()
                              caseClassDs

                              .write
                              .format("json")
                              .option("dateFormat", "yyyy-MM-dd")
                              .mode(SaveMode.Overwrite)
                              .save("hdfs://172.16.10.15:8020/result")


      }

        def initifile(): Unit =
        {
              import spark.implicits._
              var c =generate("failed").toString
              val cls=Seq(c).toDS()

              cls.write
                .format("json")
                .option("dateFormat", "yyyy-MM-dd")
                .mode(SaveMode.Overwrite)
                .save("hdfs://172.16.10.15:8020/result")
        }



}
