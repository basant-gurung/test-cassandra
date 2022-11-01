package com.pkware.cassandra

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.io.Source

/* Created by basant.gurung on 28-10-2022 */
class VerifyCassandra(filePath: String) {
  val logger: Logger = Logger.getLogger(getClass.getName)

  val prop = new Properties
  prop.load(Source.fromFile(filePath).bufferedReader())
  val keyspaceName: String = prop.getProperty("pk.cassandra.keyspace")
  val tableName: String = prop.getProperty("pk.cassandra.table")
  val fetchLimit: String = prop.getProperty("pk.cassandra.fetch.limit")
  val host: String = prop.getProperty("spark.cassandra.connection.host")
  val user: String = prop.getProperty("spark.cassandra.auth.username")
  val pwd: String = prop.getProperty("spark.cassandra.auth.password")

  val systemKeyspaces = List("system_auth", "system_schema", "dse_system",
    "dse_leases", "dse_insights", "dse_insights_local",
    "system_distributed", "system", "dse_perf", "system_traces", "dse_security",
    "system_traces", "solr_admin", "dse_system_local", "system_backups")

  def method0(): Unit = {
    val cluster = Connection.getConnection(prop)
    try {
      val session = cluster.connect()
      logger.info("method0 ------------------> Got session using Standalone Client! " + session)
      if (session != null) {
        val metadata = session.getCluster.getMetadata
        val it = metadata.getKeyspaces.iterator()
        while (it.hasNext) {
          val keyspace = it.next().getName
          if (!systemKeyspaces.contains(keyspace)) {
            logger.info(s"method0 ------------------> KeySpace: $keyspace =>")
            val keyspaceMetadata = metadata.getKeyspace("\"" + keyspace + "\"")
            val _it = keyspaceMetadata.getTables.iterator()
            val tables = new ListBuffer[String]
            while (_it.hasNext)
              tables += _it.next().getName
            logger.info(s"method0 --------> Tables: ${tables.toList}")
          }
        }
      }
      session.close()
    } catch {
      case e: Exception => logger.error(e)
    } finally {
      if (cluster != null) {
        cluster.close()
      }
    }
  }

  def method1(): Unit = {
    val cluster = Connection.getConnection(prop)
    try {
      val session = cluster.connect()
      logger.info("method1 ------------------> Trying to read data using method #1..." + session)
      if (session != null) {
        val res = session.execute(s"select * from $keyspaceName.$tableName limit $fetchLimit")
        val it = res.iterator()
        while (it.hasNext) {
          logger.info("method1 ------------------> " + it.next().toString)
        }
      }
      session.close()
    } catch {
      case e: Exception => logger.error(e)
    } finally {
      if (cluster != null) {
        cluster.close()
      }
    }
  }

  def method2(spark: SparkSession): Unit = {
    logger.info("method2 ------------------> Trying to read data using method #2...")
    try {
      val df = spark
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName, "keyspace" -> keyspaceName))
        .load()
      show(2, df)
    } catch {
      case e: Throwable => logger.error(e)
    }
  }

  def method3(spark: SparkSession): Unit = {
    logger.info("method3 ------------------> Trying to read data using method #3 ...")
    try {
      val df = spark.sql(s"select * from $keyspaceName.$tableName limit $fetchLimit")
      show(3, df)
    } catch {
      case e: Throwable => logger.error(e)
    }
  }

  def show(method: Int, df: DataFrame): Unit = {
    if (df == null) {
      logger.info(s"method$method ------------------> No Data Returned")
      return
    }

    val totalRowCount: Long = df.count()
    logger.info(s"method$method ------------------> Total Rows Count :: " + totalRowCount)
    if (totalRowCount == 0)
      return

    logger.info(s"method$method ------------------> Fetching $fetchLimit rows ...")
    val sampleData = df.limit(fetchLimit.toInt)
    sampleData.collect().foreach(row => logger.info(s"method$method ------------------> " + row.mkString("|")))
  }

  def getSparkConf: SparkConf = { //GET SPARK SESSION
    val conf = new SparkConf()
    try {
      conf
        .setMaster(prop.getProperty("spark.master"))
        .setAppName(prop.getProperty("spark.app.name"))
        .set("spark.app.id", "DB Connection Verification App " + System.currentTimeMillis())
        .set("spark.scheduler.mode", "FAIR")
        .set("sun.java.command", "PKWare has masked this to hide sensitive Info related to source connection")
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.auth.username", user)
        .set("spark.cassandra.auth.password", pwd)

      if (!"0".equals(prop.getProperty("pk.ssl.type"))) {
        conf
          .set("spark.cassandra.connection.ssl.enabled", "true")
          .set("spark.cassandra.connection.timeout_ms", "10000")
          .set("spark.cassandra.connection.ssl.trustStore.type", "JKS")
          .set("spark.cassandra.connection.ssl.protocol", "TLS")
          .set("spark.cassandra.connection.ssl.enabledAlgorithms", "TLS_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA")
          .set("spark.cassandra.connection.ssl.trustStore.path", prop.getProperty("pk.trustStore.path"))
          .set("spark.cassandra.connection.ssl.trustStore.password", DgSecurity.decryptProperty(prop.getProperty("pk.trustStore.password")))

        if ("2".equals(prop.getProperty("pk.ssl.type"))) { // Two Way SSL
          conf
            .set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
            .set("spark.cassandra.connection.ssl.keyStore.type", "JKS")
            .set("spark.cassandra.connection.ssl.keyStore.path", prop.getProperty("pk.keyStore.path"))
            .set("spark.cassandra.connection.ssl.keyStore.password", DgSecurity.decryptProperty(prop.getProperty("pk.keyStore.password")))
        }
      }
      val keys = prop.stringPropertyNames().toArray()
      keys.filter(_.toString.startsWith("custom."))
        .map(_.toString.replace("custom.", ""))
        .foreach(key => conf.set(key, prop.getProperty("custom.".concat(key))))

      logger.info("------------------> Loading configurations...")
      conf.getAll.sortBy(_._1).foreach(logger.info(_))
    } catch {
      case e: Exception =>
        logger.error(e)
        throw e
    }
    conf
  }

  def start(): Unit = {
    logger.info("------------------> Starting Program...")
    method0()
    method1()
    val spark = SparkSession.builder().config(getSparkConf).getOrCreate()
    try {
      method2(spark)
      //method3(spark)
    } finally {
      logger.info("------------------> Stopping Program...")
      spark.stop()
    }
  }
}
