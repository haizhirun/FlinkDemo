package transformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Transformation08 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.readTextFile("test00.txt")
    val streamFlatMap1 = stream1.flatMap(x => x.split(" "))
    val stream2 = env.readTextFile("test01.txt")
    val streamFlatMap2 = stream2.flatMap(x => x.split(" "))
    val streamConnect = streamFlatMap1.union(streamFlatMap2)
    streamConnect.print()
    env.execute("FirstJob")
  }
}
