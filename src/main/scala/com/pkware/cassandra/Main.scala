package com.pkware.cassandra

/* Created by basant.gurung on 01-11-2022 */
object Main {
  def main(args: Array[String]): Unit = {
    println("Starting test-cassandra")
    val filePath = if (args.length > 0) args(0) else "spark-cassandra.conf"
    new VerifyCassandra(filePath).start()
    println("Finished test-cassandra. Log file name : test-cassandra-1.0-app.log")
  }
}