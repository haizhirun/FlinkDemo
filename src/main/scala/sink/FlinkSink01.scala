package sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08

object FlinkSink01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.readTextFile("test00.txt")

    val myProducer = new FlinkKafkaProducer08[String](
      "hadoop102:9092",         // broker list
      "FlinkSink01",               // target topic,运行之前可以查看kafka上是否有FlinkSink01 topic
      new SimpleStringSchema)   // serialization schema

    stream.addSink(myProducer)
    env.execute("FlinkSink01...")
  }
}
