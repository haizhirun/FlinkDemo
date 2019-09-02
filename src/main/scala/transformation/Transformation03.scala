package transformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Transformation03 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.generateSequence(1,10)
    val streamFilter = stream.filter{
      x => x == 1
    }
    streamFilter.print()

    env.execute("FirstJob")
  }
}
