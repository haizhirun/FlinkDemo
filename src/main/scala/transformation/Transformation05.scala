package transformation
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Transformation05 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.readTextFile("test00.txt")
    val streamFlatMap = stream1.flatMap(x => x.split(" "))
    val stream2 = env.fromCollection(List(1,2,3,4))
    val streamConnect: ConnectedStreams[String, Int] = streamFlatMap.connect(stream2)

    val streamCoMap: DataStream[Any] = streamConnect.map(
      (str) => str + "connect",
      (in) => in + 100
    )
    streamCoMap.print()
    env.execute("FirstJob")
  }
}
