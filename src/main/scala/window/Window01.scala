package window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
object Window01 {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 创建SocketSource
    val stream = env.socketTextStream("localhost", 11111)

    // 对stream进行处理并按key聚合
    val streamKeyBy = stream.map(item => (item,1)).keyBy(0)

    // 引入滚动窗口,注：count指的是单个key的数量达到count才会触发CountWindow的执行
    // 这里的5指的是5个相同key的元素计算一次
    val streamWindow = streamKeyBy.countWindow(5)

    // 执行聚合操作
    val streamReduce = streamWindow.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )

    // 将聚合数据写入文件
    streamReduce.print()

    // 执行程序
    env.execute("TumblingWindow")

  }
}
