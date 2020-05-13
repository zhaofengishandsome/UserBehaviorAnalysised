package com.foo

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)
object TrafficAnalysis {

  def main(args: Array[String]): Unit = {

   val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      //  以 window 下为例，需替换成自己的路径
      .readTextFile("E:\\Intellij IDEA-workspace\\UserBehaviorAnalysised\\NetworktrafficAnalysis\\src\\main\\resources\\apache.txt")
      .map(line => {
        val linearray = line.split(" ")
        val dtf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = dtf.parse(linearray(3)).getTime

        ApacheLogEvent(linearray(0), linearray(2), timestamp, linearray(5),
          linearray(6))
      })
      .assignAscendingTimestamps(_.eventTime)
      .keyBy("url")
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new ApacheLogCountAgg(), new ApacheLogWindowResultFunction())
      .keyBy(1)
      .process(new ApacheLogTopNHotItems(5))
      .print()
    env.execute("Traffic Analysis Job")
  }
  class ApacheLogCountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L
    override def add(apacheLogEvent: ApacheLogEvent, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }
  class ApacheLogWindowResultFunction extends WindowFunction[Long, UrlViewCount,
    Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long],
                       collector: Collector[UrlViewCount]) : Unit = {
      val url: String = key.asInstanceOf[Tuple1[String]].f0
      val count = aggregateResult.iterator.next
      collector.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

  class ApacheLogTopNHotItems extends KeyedProcessFunction[Tuple, UrlViewCount,
    String] {
    private var topSize = 0
    def this(topSize: Int) {
      this()
      this.topSize = topSize
    }
    private var itemState : ListState[UrlViewCount] = _
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val itemsStateDesc = new ListStateDescriptor[UrlViewCount]("itemState-state",
        classOf[UrlViewCount])
      itemState = getRuntimeContext.getListState(itemsStateDesc)
    }
    override def processElement(input: UrlViewCount, context:
    KeyedProcessFunction[Tuple, UrlViewCount, String]#Context, collector:
                                Collector[String]): Unit = {
      //  每条数据都保存到状态中
      itemState.add(input)
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple,
      UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //  获取收到的所有 URL 访问量
      val allItems: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems += item
      }
      //  提前清除状态中的数据，释放空间
      itemState.clear()
      //  按照访问量从大到小排序
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      //  将排名信息格式化成 String,  便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append(" 时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      for (i <- sortedItems.indices) {
        val currentItem: UrlViewCount = sortedItems(i)
        // e.g. No1 ： URL =/blog/tags/firefox?flav=rss20 流量 =55
        result.append("No").append(i+1).append(":")
          .append(" URL=").append(currentItem.url)
          .append(" 流量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      //  控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}
