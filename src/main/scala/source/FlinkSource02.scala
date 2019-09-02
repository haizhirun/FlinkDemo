package source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlinkSource02 {
  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.获取数据源(source)
    val source: DataStream[String] = env.socketTextStream("hadoop102",11111)

    //3.打印数据（sink）
    source.print()

    //4.执行任务
    env.execute("FlinkSource02...")


  }

}
