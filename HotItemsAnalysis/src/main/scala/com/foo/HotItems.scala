package com.foo

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 实时热门商品统计模块
 * 1.抽取出业务时间戳，用业务时间做窗口
 * 2.过滤出点击行为
 * 3.滑动窗口
 * 按每个窗口进行聚合，输出每个窗口中点击量前n名的商品
 */

//进行封装
case class UserBehavior(userId: Long,itemsId: Long,categoryId: Int,behavior: String,timestamp: Long)

//商品点击量(窗口操作输出的类型)
case class ItemViewCount(itemsId: Long,windowEnd: Long,count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {

    //创建flink的流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间为eventtime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node32:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //读取本地文件
    val stream = env
//      .readTextFile("E:\\Intellij IDEA-workspace\\UserBehaviorAnalysised\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
        .addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
        .map(line=>{
        val lineArray = line.split(",")
        UserBehavior(lineArray(0).toLong,lineArray(1).toLong,lineArray(2).toInt,lineArray(3),lineArray(4).toLong)
      })

    //指定时间戳和watermark
        .assignAscendingTimestamps(_.timestamp * 1000)
    //过滤点击事件
        .filter(_.behavior=="pv")
    //设置窗口滑动和统计点击量
        .keyBy("itemsId")
        .timeWindow(Time.hours(1),Time.minutes(5))

        .aggregate(new countAgg(),new windowResultFunction())

      //计算热门topN的商品
        .keyBy("windowEnd")
        .process(new TopNHotItems(3))//求出点击量前3的商品


    stream.print()

    env.execute("Hot Items Job")
  }

}

//count统计的聚合函数实现，没出现一条记录就加1
class countAgg() extends AggregateFunction[UserBehavior,Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

//用于输出窗口的结果
class windowResultFunction() extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    val itemsId : Long = key.asInstanceOf[Tuple1[Long]].f0

    val count = input.toIterator.next()

    out.collect(ItemViewCount(itemsId,window.getEnd,count))
  }
}

//求出某个窗口前topN的热门点击商品，key为时间戳，输出topN的结果为字符串
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String] {

  private var itemState : ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    //命名状态变量的名字和状态变量的类型
    val itemsStateDesc  = new ListStateDescriptor[ItemViewCount]("itemState-state",classOf[ItemViewCount])

    //定义状态的变量
    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }



  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每条数据都保存到状态中
    itemState.add(value)
    //注意windowEnd+1的eventtime timer，当触发时，说明收齐了属于wiindowEnd窗口的所以窗口数据
    //也就是当程序看到windowEnd+1 的水位线watermark时，触发ontime回调函数
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //获取收到的所以商品点击量
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()

    import  scala.collection.JavaConversions._

    for (item <- itemState.get()){
      allItems += item
    }

    //提前清除状态中的数据，释放空间

    itemState.clear()

    //按照点击量从大到小排序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //将排名信息格式化成String，便于打印

    val result : StringBuilder = new StringBuilder

    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedItems.indices){

      val currentItem : ItemViewCount = sortedItems(i)

      //e.g No1: 商品ID=12224 游览量=2413
      result.append("No").append(i+1).append(":")
        .append(" 商品ID=").append(currentItem.itemsId)
        .append(" 游览量=").append(currentItem.count).append("\n")

    }

    result.append("===================================\n\n")

    //控制台输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString())

  }


}
