package transformation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Transformation11 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.readTextFile("test00.txt").flatMap(item => item.split(" ")).map(item => (item, 1)).keyBy(0)
    val streamReduce = stream.fold(100)(
      (begin, item) => (begin + item._2)
    )


    env.execute("FirstJob")
  }
}
