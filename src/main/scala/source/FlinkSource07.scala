package source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.api.scala._

object FlinkSource07 {
  def main(args: Array[String]): Unit = {
    val broker = "hadoop102:9092"
    val zookeeper = "hadoop102:2181"
    val group = "im-push-keeper"
    val inTopic = "KafkaApiStatTopic"

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", broker)
    kafkaProps.setProperty("zookeeper.connect", zookeeper)
    kafkaProps.setProperty("group.id", group)

    val consumer =  new FlinkKafkaConsumer08[String](inTopic, new SimpleStringSchema(), kafkaProps)
    //从头开始读取
    consumer.setStartFromEarliest()

    val stream = env.addSource(consumer)
    stream.print()

    env.execute("FlinkSource07...")

  }
}
