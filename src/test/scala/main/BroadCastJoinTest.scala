package main

import java.lang

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

class BroadCastJoinTest extends SparkTest {

  import testImplicits._

  test("Joining Two smaller Data Sets") {
    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    //by default 10 MB for AUTO_BROADCASTJOIN_THRESHOLD
    //If size less than 10 MB of either DF then that will be broad casted
    //spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key,-1)
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()
    val df2: Dataset[lang.Long] = spark.range(100).as("b")
    val df1 = spark.range(100).as("a")
    val joinedDF = df1.join(df2).where($"a.id" === $"b.id")
    assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
//    println(joinedDF.queryExecution.sparkPlan)
//    println(joinedDF.explain())
  }

  test("Joining one large with another large Data Set") {

    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()

    val peopleDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people.csv"))

    val peopleDF2 = spark.read.option("header", "true")
      .csv(Resource.pathOf("people.csv"))

    //Joining 2 large data sets should not ideally result in Broadcast, shuffle is ok in this case
    val joinedDF = peopleDF.join(peopleDF2, Seq("firstName"), Inner)
    assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 0)
//    println(joinedDF.explain())
//    println(joinedDF.queryExecution.sparkPlan)
  }

  test("Joining one large with one medium Data Sets") {

    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()

    val peopleDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people.csv"))

    val peopleSmallDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people_small.csv"))

    println(peopleDF.queryExecution.logical.stats.sizeInBytes)
    println(peopleSmallDF.queryExecution.logical.stats.sizeInBytes)
    //Size of both the csv > 3mb so no broadcast, but if we increase size then possible
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> SizeInMBs(4L).toBytes.toString) {
      val joinedDF = peopleDF.join(peopleSmallDF, Seq("firstName"), Inner)
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
    }
  }
  import org.apache.spark.sql.functions._


  test("Joining Two smaller Data Sets with broadcast threshold off") {
    val thresholdSettings = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toLong
    SizeInBytes(thresholdSettings).toMBs.prettyPrint()
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"/*SizeInMBs(1L).toBytes.toString*/) {
      val df2 = spark.range(100).as("b")
      val df1 = spark.range(100).as("a")
      //broadcast() function can override settings used above
      val joinedDF = df1.join(broadcast(df2)).where($"a.id" === $"b.id")
//      println(joinedDF.explain())
//      println(joinedDF.queryExecution.sparkPlan)
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
      // Please don't change below assert to fix test
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: SortMergeJoinExec => p }.size === 0)

    }
  }

  test("Cross Joining Two Data Sets") {
    val peopleDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people.csv"))

    val peopleSmallDF = spark.read.option("header", "true")
      .csv(Resource.pathOf("people_small.csv"))

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> SizeInMBs(3L).toBytes.toString) {
      val joinedDF = peopleDF.crossJoin(broadcast(peopleSmallDF))
      assert(joinedDF.queryExecution.sparkPlan.collect { case p: BroadcastNestedLoopJoinExec => p }.size === 1)
    }
  }

  // TODO for attendees:
  // 1. Check output of joinedDF.explain()
  // Check this spark class to find out how JoinSelection works.
  // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala
  // Especially CanBroadCast,canBuildRight,canBuildLeft methods
  // Look at: https://www.coursera.org/lecture/big-data-analysis/optimizing-joins-VZmLX
}
