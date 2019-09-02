package transformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Transformation09 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("test00.txt")
    val streamFlatMap = stream.flatMap{
      x => x.split(" ")
    }
    val streamMap = streamFlatMap.map{
      x => (x,1)
    }
    val streamKeyBy = streamMap.keyBy(0)
    streamKeyBy.print()
    env.execute("FirstJob")
  }
}
