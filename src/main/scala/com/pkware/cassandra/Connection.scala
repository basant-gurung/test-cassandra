package com.pkware.cassandra

import com.datastax.driver.core.{Cluster, JdkSSLOptions, SocketOptions}

import java.security.SecureRandom
import java.util.Properties
import javax.net.ssl.{KeyManagerFactory, SSLContext}

/* Created by basant.gurung on 31-10-2022 */
object Connection {

  def getConnection(prop: Properties): Cluster = {
    val socketOptions: SocketOptions = new SocketOptions
    socketOptions.setConnectTimeoutMillis(prop.getProperty("cassandra.connect.timeout").toInt)
    socketOptions.setReadTimeoutMillis(prop.getProperty("cassandra.read.timeout").toInt)

    import javax.net.ssl.TrustManagerFactory
    var cluster: Cluster = null

    if (!"0".equals(prop.getProperty("pk.ssl.type"))) {
      import java.io.FileInputStream
      import java.security.KeyStore
      val tks = KeyStore.getInstance("JKS")
      tks.load(new FileInputStream(prop.getProperty("pk.trustStore.path")),
        DgSecurity.decryptProperty(prop.getProperty("pk.trustStore.password")).toCharArray)
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(tks)

      var kmf: KeyManagerFactory = null
      if ("2".equals(prop.getProperty("pk.ssl.type"))) {
        val kks = KeyStore.getInstance("JKS")
        kks.load(new FileInputStream(prop.getProperty("pk.keyStore.path")),
          DgSecurity.decryptProperty(prop.getProperty("pk.keyStore.password")).toCharArray)
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        kmf.init(kks, DgSecurity.decryptProperty(prop.getProperty("pk.keyManager.password")).toCharArray)
      }

      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(if (kmf != null) kmf.getKeyManagers else null,
        if (tmf != null) tmf.getTrustManagers else null, new SecureRandom()
      )
      val sslOptions = JdkSSLOptions.builder.withSSLContext(sslContext).build

      cluster = Cluster.builder.addContactPointsWithPorts(
        new java.net.InetSocketAddress(prop.getProperty("spark.cassandra.connection.host"), prop.getProperty("spark.cassandra.connection.port").toInt))
        .withCredentials(prop.getProperty("spark.cassandra.auth.username"), prop.getProperty("spark.cassandra.auth.password"))
        .withSSL(sslOptions)
        .withSocketOptions(socketOptions)
        .build
    } else {
      cluster = Cluster.builder()
        .withoutMetrics()
        .addContactPoint(prop.getProperty("spark.cassandra.connection.host"))
        .withPort(prop.getProperty("spark.cassandra.connection.port").toInt)
        .withCredentials(prop.getProperty("spark.cassandra.auth.username"), prop.getProperty("spark.cassandra.auth.password"))
        .withSocketOptions(socketOptions)
        .build()
    }
    cluster
  }
}
