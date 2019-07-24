# hdfs-spark-cassandra
ensure good connectivity between apache spark and apache hdfs and apache cassandra (read from hdfs / process with spark / write in cassandra)

#	Application Interaction Spark/HDFS/Cassandra

Cette application a pour but tester les interactions entre Apache Spark et Apache HDFS et Apache Cassandra, elle permet :

-	La lecture d’un fichier csv depuis HDFS. 
-	Faire des traitements Spark sur le fichier csv créer.
-	La création d’un KEYSPACE et la définition de sa stratégie sur Cassandra. 
-	La création d’une table dans le KEYSPACE créer.
-	Le remplissage de la table.
-	La lecture des données écrit sur Cassandra.
-	La vérification des données écrit. 
-	Générer un fichier résultat en format JSON, qui contient les informations du test et son résultat. 

#	Composants concernés


Composant	   	Version
- Spark		2.3.2
- HDFS		2.6.0
- Cassandra		3.11.4
- DC-OS		1.12
- Scala		2.11.8
- Assembly		0.14.9





# Prérequis 
Avant de lancer l’application vous devez la configurer, cela se fait au niveau du fichier de configuration de l’application, qui est dans le chemin (/src/main/resources/Cassandra.conf).


Traitements : 
-	Lancer l’application sur le dcos bootstrap avec la commande 
dcos spark --name="spark-2-3" run --submit-args="--conf spark.mesos.containerizer=docker --conf spark.driver.memory=4G --conf spark.cores.max=3 --conf spark.executor.cores=1 --conf spark.executor.memory=4G --conf spark.mesos.executor.docker.forcePullImage=false --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs:///spark_history  --class hdfsToCassandra hdfs:///jars/HDFS-Spark-Cassandra-assembly-0.1.jar"
 
Validation du test : Vérifier le statut du test dans le fichier résultat. 
