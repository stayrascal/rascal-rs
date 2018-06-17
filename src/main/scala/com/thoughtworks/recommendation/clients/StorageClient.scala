package com.thoughtworks.recommendation.clients

import grizzled.slf4j.Logging
import org.apache.predictionio.data.storage.{BaseStorageClient, StorageClientConfig, StorageClientException}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.ConnectTransportException

class StorageClient(val config: StorageClientConfig) extends BaseStorageClient with Logging {
  override val client = try {
    val hosts = config.properties.get("HOSTS").map(_.split(",").toSeq).getOrElse(Seq("localhost"))
    val ports = config.properties.get("PORTS").map(_.split(",").toSeq.map(_.toInt)).getOrElse(Seq(9300))
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.properties.getOrElse("CLUSTERNAME", "elasticsearch"))
    val transportClient = new TransportClient(settings)
    (hosts zip ports).foreach { hp =>
      transportClient.addTransportAddress(new InetSocketTransportAddress(hp._1, hp._2))
    }
    transportClient
  } catch {
    case e: ConnectTransportException => throw new StorageClientException(e.getMessage, e)
  }
}
