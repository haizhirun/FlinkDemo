package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._


object FlinkSource03 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val list = List(1,2,3,4)
    val stream = env.fromCollection(list)
    stream.print()
    env.execute("FlinkSource03...")
  }
}
