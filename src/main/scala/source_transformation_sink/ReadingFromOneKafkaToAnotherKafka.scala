package source_transformation_sink

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08, FlinkKafkaProducer08}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * 纯粹用来讲集群上的topic迁移到本地集群上
  */
object ReadingFromOneKafkaToAnotherKafka {
  private var customPartitioner:FlinkKafkaPartitioner[String] = _
  private val ZOOKEEPER_HOST = "121.41.45.182:2181"
  private val KAFKA_BROKER = "121.41.45.182:9092"
  private val TRANSACTION_GROUP = "test_local"
  private val KAFKA_TOPICS = List("KafkaApiSendErrorTopic","KafkaApiStatTopic","KafkaFeedStatTopic","KafkaOriginTopic",
    "KafkaPIPETopic","KafkaPushsFlinkTopic","KafkaPushsTopic","KafkaStorageTopic",
    "KafkaStorageTopicV2","account_online_status_refresh","async_task_q","event_elastic")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.getConfig.disableSysoutLogging()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    env.setParallelism(1)
    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", "121.41.45.182:2181")
    kafkaProps.setProperty("bootstrap.servers", "121.41.45.182:9092")
    kafkaProps.setProperty("group.id", "test_local")

    //topicd的名字是new，schema默认使用SimpleStringSchema()即可
    val stream = env
      .addSource(
        new FlinkKafkaConsumer08[String]("event_elastic", new SimpleStringSchema(), kafkaProps).setStartFromEarliest()
      )


    val kafkaProps2 = new Properties()
    kafkaProps2.setProperty("zookeeper.connect", "hadoop102:2181")
    kafkaProps2.setProperty("bootstrap.servers", "hadoop102:9092")
    kafkaProps2.setProperty("group.id", "test_local")
    val sinker: FlinkKafkaProducer08[String] = new FlinkKafkaProducer08[String]("event_elastic",new SimpleStringSchema(),kafkaProps2,customPartitioner)

    stream.addSink(sinker)

    stream.print()

    env.execute("ReadingFromOneKafkaToAnotherKafka")
  }

}
