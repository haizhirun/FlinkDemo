package transformation
import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Transformation04 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("test00.txt")

    val streamMap = stream.flatMap(item => item.split(" ")).filter(item => item.equals("hadoop"))
    val streamCollect = env.fromCollection(List(1,2,3,4))

    val streamConnect: ConnectedStreams[String, Int] = streamMap.connect(streamCollect)
    streamConnect.map(item=>println(item), item=>println(item))

    env.execute("FirstJob")
  }

}
