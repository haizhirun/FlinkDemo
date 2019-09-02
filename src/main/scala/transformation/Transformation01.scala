package transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object Transformation01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.generateSequence(1,10)
    val streamMap = stream.map { x => x * 2 }
    streamMap.print()

    env.execute("FirstJob")
  }
}
