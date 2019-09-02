package source
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkSource06 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1,10)
    stream.print()
    env.execute("FlinkSource06...")
  }

}
