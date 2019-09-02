package source_transformation_sink

import java.nio.file.FileSystem

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
  * 批处理WordCount演示
  */
object WordCountBatch {
  def main(args: Array[String]): Unit = {
    //构造执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)  //设置并行度为1，便于输出在一个文件夹中
    //读取本地文件
    val input = "test00.txt"
    val ds: DataSet[String] = env.readTextFile(input)
    // 其中flatMap 和Map 中  需要引入隐式转换
    import org.apache.flink.api.scala.createTypeInformation
    //经过groupby进行分组，sum进行聚合
    val aggDs: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    // 打印
    aggDs.print()

    aggDs.writeAsText("WordCountBatchOutput"+System.currentTimeMillis() +".txt")

    env.execute("WordCountBatch...")
  }
}
