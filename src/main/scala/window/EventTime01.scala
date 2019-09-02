package window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object EventTime01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 从调用时刻开始给env创建的每一个stream追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //指定eventTime提取方式和waterMark
    val stream = env.socketTextStream("localhost", 11111).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
      override def extractTimestamp(t: String): Long = {
        val eventTime: Long = t.split(" ")(0).toLong
        println(eventTime) //打印出时间方便调试
        eventTime
      }
    }).map(item => (item.split(" ")(1),1L)).keyBy(0)

    val streamWindow: WindowedStream[(String, Long), Tuple, TimeWindow] = stream.window(TumblingEventTimeWindows.of(Time.seconds(5)))

    val streamReduce = streamWindow.reduce((item1, item2) =>
      (item1._1, item1._2 + item2._2)
    )

    streamReduce.print()

    env.execute("EventTime01")

  }
}
