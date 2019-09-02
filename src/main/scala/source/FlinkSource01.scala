package source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlinkSource01 {
  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.获取数据源(source)
    val source: DataStream[String] = env.readTextFile("test00.txt")


    //3.打印数据（sink）
    source.print()

    //4.执行任务
    env.execute("FlinkSource01...")


  }

}
