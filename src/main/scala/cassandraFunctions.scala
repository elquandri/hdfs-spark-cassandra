import HdfsToCassandra.conf
import com.datastax.spark.connector.cql.CassandraConnector

class cassandraFunctions {

  def createtable(table: String, keyspace: String): Unit ={



    CassandraConnector(conf).withSessionDo { session =>

      session.execute("DROP KEYSPACE IF EXISTS "+keyspace)
      session.execute("CREATE KEYSPACE "+keyspace+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE "+keyspace+"."+table+"(code ascii PRIMARY KEY, nom text, codepostale text, libelle text, ligne5 text, gps text)")
    }

  }
}
