package window

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object EventTime06 {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建SocketSource
    val stream = env.socketTextStream("localhost", 11111)

    // 对stream进行处理并按key聚合
    val streamKeyBy = stream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(3000)) {
        override def extractTimestamp(element: String): Long = {
          val sysTime = element.split(" ")(0).toLong
          println(sysTime)
          sysTime
        }}).map(item => (item.split(" ")(1), 1)).keyBy(0)

    // 引入滚动窗口
    val streamWindow = streamKeyBy.window(EventTimeSessionWindows.withGap(Time.seconds(5)))

    // 执行聚合操作
    val streamReduce = streamWindow.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )

    // 将聚合数据写入文件
    streamReduce.print

    // 执行程序
    env.execute("TumblingWindow")
  }
}
