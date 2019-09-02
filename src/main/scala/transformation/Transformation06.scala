package transformation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Transformation06 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.readTextFile("test00.txt")
    val stream2 = env.readTextFile("test01.txt")
    val streamConnect = stream1.connect(stream2)
    val streamCoMap = streamConnect.flatMap(
      (str1) => str1.split(" "),
      (str2) => str2.split(" ")
    )
    streamConnect.map(item=>println(item), item=>println(item))

    env.execute("FirstJob")
  }
}
