package transformation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Transformation12 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("test02.txt").map(item => (item.split(" ")(0), item.split(" ")(1).toLong)).keyBy(0)

    val streamReduce = stream.sum(1)

    streamReduce.print()

    env.execute("FirstJob")

  }
}
