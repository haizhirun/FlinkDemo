package source_transformation_sink

import java.time.ZoneId
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.api.scala._

/**
  * 从kafka读取数据然后存取到hdfs中
  */
object ReadFromKafkaToHdfs {
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


    // dStream.writeAsText("file:///WordCountStreaming"+System.currentTimeMillis()+".txt")
    val sink = new BucketingSink[String]("hdfs://hadoop102:9000/user/mrpyq/test_topic")
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd-HH-mm", ZoneId.of("Asia/Shanghai")))
      sink.setWriter(new StringWriter())
    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins

    stream.addSink(sink)

    env.execute("ReadFromKafkaToHdfs...")
  }
}
