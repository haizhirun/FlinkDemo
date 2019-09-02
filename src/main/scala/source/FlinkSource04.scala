package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object FlinkSource04 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val iterator = Iterator(1,2,3,4)
    val stream = env.fromCollection(iterator)
    stream.print()
    env.execute("FlinkSource04...")
  }
}
