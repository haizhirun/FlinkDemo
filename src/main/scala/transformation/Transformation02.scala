package transformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object Transformation02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("test00.txt")

    val streamFlatMap = stream.flatMap{
      x => x.split(" ")
    }

    streamFlatMap.print()

    env.execute("FirstJob")
  }
}
