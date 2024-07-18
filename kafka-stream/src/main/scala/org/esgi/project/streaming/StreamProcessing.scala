package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.esgi.project.streaming.models.{Like, View}

import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"movie-stats-app"

  private val props: Properties = buildProperties()

  // defining topoligie
  val builder: StreamsBuilder = new StreamsBuilder

  val viewsTopic = "views"
  val viewsStoreName = "views-store"

  val likesTopic = "likes"
  val likesStoreName = "likes-store"

  val viewsForLastFiveMinutesStoreName = "viewsForLastFiveMinutes"

  val views = builder.stream[String, View](viewsTopic)
  val likes = builder.stream[String, Like](likesTopic)

  val viewCounts: KTable[String, Long] = views
    .groupBy((_, view) => view.id.toString)
    .count()(Materialized.as(viewsStoreName))

  val likeScoreCounts: KTable[String, (Double, Long)] = likes
    .groupBy((_, like) => like.id.toString)
    .aggregate[(Double, Long)](
      initializer = (0.0, 0L)
    )((key, newLike, agg) => ((agg._1 * agg._2 + newLike.score) / (agg._2 + 1), agg._2 + 1))(
      Materialized.as(likesStoreName)
    )

  val viewsCountForLastFiveMinutes: KTable[Windowed[String], Long] = views
    .groupBy((_, view) => view.id.toString)
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(viewsForLastFiveMinutesStoreName))

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

  // auto loader from properties file in project
  def buildProperties(appName: Option[String] = None): Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appName.getOrElse(applicationName))
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
}
