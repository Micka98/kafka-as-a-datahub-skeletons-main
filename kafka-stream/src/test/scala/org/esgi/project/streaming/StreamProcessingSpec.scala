package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{KeyValue, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueStore, ReadOnlyWindowStore, ValueAndTimestamp}
import org.scalatest.funsuite.AnyFunSuite

import java.lang
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {

  test("Topology should compute correct view counts") {
    // Given
    val viewMessages = List(
      """{"id": 1, "title": "Kill Bill", "view_category": "start_only"}""",
      """{"id": 1, "title": "Kill Bill", "view_category": "half"}""",
      """{"id": 2, "title": "Pulp Fiction", "view_category": "full"}"""
    )

    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties(Some(UUID.randomUUID().toString))
    )

    val viewsTopic: TestInputTopic[String, String] = topologyTestDriver
      .createInputTopic(
        StreamProcessing.viewsTopic,
        Serdes.stringSerde.serializer(),
        Serdes.stringSerde.serializer()
      )

    val viewsCountStore: KeyValueStore[String, Long] =
      topologyTestDriver.getKeyValueStore[String, Long](StreamProcessing.viewsStoreName)

    val viewsForLastFiveMinutesCountStore: ReadOnlyWindowStore[String, ValueAndTimestamp[Long]] =
      topologyTestDriver.getWindowStore[String, ValueAndTimestamp[Long]](
        StreamProcessing.viewsForLastFiveMinutesStoreName
      )

    // When
    /*viewMessages.foreach { message =>
      viewsTopic.pipeInput(message)
    }*/
    val currentTime = Instant.now()

    viewMessages.zipWithIndex.foreach { case (message, index) =>
      viewsTopic.pipeInput(null, message, currentTime.plusSeconds(index * 60)) // Messages one minute apart
    }
    // Check windowed view counts for last five minutes
    val windowStartTime = currentTime.minusSeconds(300)
    val windowEndTime = currentTime

    val windowedViewCounts: List[KeyValue[lang.Long, ValueAndTimestamp[Long]]] =
      viewsForLastFiveMinutesCountStore.fetch("1", windowStartTime, windowEndTime).asScala.toList
    val totalViewsForLastFiveMinutes = windowedViewCounts.map(_.value.value()).sum

    assert(totalViewsForLastFiveMinutes == 2)

    // Then
    assert(viewsCountStore.get("1") == 2)
    assert(viewsCountStore.get("2") == 1)

    topologyTestDriver.close()
  }

  test("Topology should compute correct like scores and counts") {
    // Given
    val likeMessages = List(
      """{"id": 1, "score": 4.0}""",
      """{"id": 1, "score": 5.0}""",
      """{"id": 2, "score": 2.0}"""
    )

    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties(Some(UUID.randomUUID().toString))
    )

    val likesTopic: TestInputTopic[String, String] = topologyTestDriver
      .createInputTopic(
        StreamProcessing.likesTopic,
        Serdes.stringSerde.serializer(),
        Serdes.stringSerde.serializer()
      )

    val likesCountStore: KeyValueStore[String, (Double, Long)] =
      topologyTestDriver.getKeyValueStore[String, (Double, Long)](StreamProcessing.likesStoreName)

    // When
    likeMessages.foreach { message =>
      likesTopic.pipeInput(message)
    }

    // Then
    val (averageScore1, count1) = likesCountStore.get("1")
    val (averageScore2, count2) = likesCountStore.get("2")

    assert(averageScore1 == 4.5)
    assert(count1 == 2)
    assert(averageScore2 == 2.0)
    assert(count2 == 1)

    topologyTestDriver.close()
  }
}
