package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  val correctRow = new {
    val stringValues = List("columnA", "2.56", "3.51")
    val values = List("columnA", 2.56, 3.51)
    val types = Array(StringType, DoubleType, DoubleType)
    val schema = StructType(stringValues.zip(types).map(x => StructField(x._1, x._2, false)))
  }

  val correctData = new {
    val testDataFilename = "/timeusage/TimeUsageTestData.csv"
    val needsNames = List("t0115", "t180305")
    val workNames = List("t051", "t18051")
    val otherNames = List("t1804")
    val names = Array("working", "sex", "age", "primaryNeeds", "work", "other")
    val types = Array(StringType, StringType, StringType, DoubleType, DoubleType, DoubleType)
    val nullables = Array(false, false, false, false, false, false) //TODO: Check back on this
    val schema = StructType(names.zip(types).zip(nullables).map(x => StructField(x._1._1, x._1._2, x._2)))
    val summaryResults = Array(
      Row("working", "male", "young", 1.0, 2.0, 2.0),
      Row("working", "female", "active", 2.0, 1.0, 1.0),
      Row("not working", "female", "elder", 4.0, 4.0, 0.5),
      Row("not working", "female", "elder", 1.0, 1.0, 1.0)
    )
    val rowResults = Array(
      Row("not working", "female", "elder", 2.5, 2.5, 0.8),
      Row("working", "female", "active", 2.0, 1.0, 1.0),
      Row("working", "male", "young", 1.0, 2.0, 2.0)
    )
    val timeUsageRowResults = Array(
      TimeUsageRow("not working", "female", "elder", 2.5, 2.5, 0.8),
      TimeUsageRow("working", "female", "active", 2.0, 1.0, 1.0),
      TimeUsageRow("working", "male", "young", 1.0, 2.0, 2.0)
    )
  }

  val summaryDf = {
    val (columns, initDf) = TimeUsage.read(correctData.testDataFilename)
    val (primaryNeeds, work, other) = TimeUsage.classifiedColumns(columns)
    TimeUsage.timeUsageSummary(primaryNeeds, work, other, initDf).cache()
  }

  override def afterAll(): Unit = {
    TimeUsage.spark.stop()
  }

  test("'dfSchema' should create the DataFrame schema") {
    val schema = TimeUsage.dfSchema(correctRow.stringValues)
    assertResult(correctRow.schema)(schema)
  }

  test("'row' should create an RDD Row") {
    val row = TimeUsage.row(correctRow.stringValues)
    assertResult(correctRow.values)(row.toSeq)
  }

  test("'classifiedColumns' should split columns in three different groups") {
    val allNames = correctData.needsNames ++ correctData.workNames ++ correctData.otherNames
    val (needs, work, other) = TimeUsage.classifiedColumns(allNames)
    assertResult(correctData.needsNames.toSet)(needs.map(_.toString).toSet)
    assertResult(correctData.workNames.toSet)(work.map(_.toString).toSet)
    assertResult(correctData.otherNames.toSet)(other.map(_.toString).toSet)
  }

  test("'timeUsageSummary' should create a sum of related groups & rename values in columns") {
    val names = summaryDf.schema.fieldNames
    val results = summaryDf.collect()
    assertResult(correctData.names)(names)
    assertResult(correctData.summaryResults)(results)
  }

  test("'timeUsageGrouped' should return final results") {
    val results = TimeUsage.timeUsageGrouped(summaryDf).collect()
    assertResult(correctData.rowResults)(results)
  }

  test("'timeUsageGroupedSqlQuery' should return final results") {
    val results = TimeUsage.timeUsageGroupedSql(summaryDf).collect()
    assertResult(correctData.rowResults)(results)
  }

  ignore("'timeUsageSummaryTyped' should return a Dataset[TimeUsageRow]") { //TODO: Check back on this
    val schema = TimeUsage.timeUsageSummaryTyped(summaryDf).schema
    assertResult(correctData.schema)(schema)
  }

  test("'timeUsageGroupedTyped' should return final results") {
    val results = TimeUsage.timeUsageGroupedTyped(TimeUsage.timeUsageSummaryTyped(summaryDf)).collect()
    assertResult(correctData.timeUsageRowResults)(results)
  }
}
