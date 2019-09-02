package transformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Transformation07 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("test00.txt")
    val streamFlatMap = stream.flatMap(x => x.split(" "))
    val streamSplit = streamFlatMap.split(
        num =>
        // 字符串内容为hadoop的组成一个DataStream，其余的组成一个DataStream
       (num.equals("hadoop")) match{
        case true => List("hadoop")
        case false => List("other")
      }
    )

    val hadoop = streamSplit.select("hadoop")
    val other = streamSplit.select("other")
    hadoop.print()
//    other.print()
    env.execute("FirstJob")
  }
}
