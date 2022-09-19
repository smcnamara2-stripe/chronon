package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.Extensions._

import scala.collection.JavaConverters._
import scala.util.ScalaVersionSpecificCollectionsConverter

class StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {
  assert(Option(stagingQueryConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  private val outputTable = stagingQueryConf.metaData.outputTable
  private val tableProps = Option(stagingQueryConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .orNull

  private val partitionCols: Seq[String] = Seq(Constants.PartitionColumn) ++
    ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(
      Option(stagingQueryConf.metaData.customJsonLookUp(key = "additional_partition_cols"))
        .getOrElse(new java.util.ArrayList[String]())
        .asInstanceOf[java.util.ArrayList[String]])

  private final val StartDateRegex = replacementRegexFor("start_date")
  private final val EndDateRegex = replacementRegexFor("end_date")
  private def replacementRegexFor(literal: String): String = s"\\{\\{\\s*$literal\\s*\\}\\}"

  def computeStagingQuery(stepDays: Option[Int] = None): Unit = {
    Option(stagingQueryConf.setups).foreach(_.asScala.foreach(tableUtils.sql))
    val unfilledRange =
      tableUtils.unfilledRange(outputTable, PartitionRange(stagingQueryConf.startPartition, endPartition))

    if (unfilledRange.isEmpty) {
      println(s"""No unfilled range for $outputTable given
           |start partition of ${stagingQueryConf.startPartition}
           |end partition of $endPartition
           |""".stripMargin)
      return
    }
    val stagingQueryUnfilledRange = unfilledRange.get
    println(s"Staging Query unfilled range: $stagingQueryUnfilledRange")
    val stepRanges = stepDays.map(stagingQueryUnfilledRange.steps).getOrElse(Seq(stagingQueryUnfilledRange))
    println(s"Staging query ranges to compute: ${stepRanges.map { _.toString }.pretty}")

    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        val progress = s"| [${index + 1}/${stepRanges.size}]"
        println(s"Computing staging query for range: $range  $progress")
        val renderedQuery = stagingQueryConf.query
          .replaceAll(StartDateRegex, range.start)
          .replaceAll(EndDateRegex, range.end)
        println(s"Rendered Staging Query to run is:\n$renderedQuery")
        val df = tableUtils.sql(renderedQuery)
        tableUtils.insertPartitions(df, outputTable, tableProps, partitionCols)
        println(s"Wrote to table $outputTable, into partitions: $range $progress")
    }
    println(s"Finished writing Staging Query data to $outputTable")
  }
}

object StagingQuery {
  def main(args: Array[String]): Unit = {
    val parsedArgs = new Args(args)
    parsedArgs.verify()
    val stagingQueryConf = parsedArgs.parseConf[api.StagingQuery]
    val stagingQueryJob = new StagingQuery(
      stagingQueryConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"staging_query_${stagingQueryConf.metaData.name}"))
    )
    stagingQueryJob.computeStagingQuery(parsedArgs.stepDays.toOption)
  }
}
