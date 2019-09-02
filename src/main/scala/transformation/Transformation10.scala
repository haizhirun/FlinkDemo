package transformation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Transformation10 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("test00.txt").flatMap(item => item.split(" ")).map(item => (item, 1)).keyBy(0)

    val streamReduce = stream.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )

    streamReduce.print()

    env.execute("FirstJob")
  }
}
