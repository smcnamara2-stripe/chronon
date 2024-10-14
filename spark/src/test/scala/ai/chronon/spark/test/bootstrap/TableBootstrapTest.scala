/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test.bootstrap

import org.slf4j.LoggerFactory
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Comparison, SparkConstants, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{After, Before, Test}

import scala.util.ScalaJavaConversions.JListOps

class TableBootstrapTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = SparkSessionBuilder.build("BootstrapTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

  // Create bootstrap dataset with randomly overwritten data
  def buildBootstrapPart(queryTable: String,
                         namespace: String,
                         tableName: String,
                         columnName: String = "unit_test_user_transactions_amount_dollars_sum_30d",
                         samplePercent: Double = 0.8): (BootstrapPart, DataFrame) = {
    val bootstrapTable = s"$namespace.$tableName"
    val preSampleBootstrapDf = spark
      .table(queryTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as(columnName),
        col("ds")
      )

    val bootstrapDf = if (samplePercent < 1.0) {
      preSampleBootstrapDf.sample(samplePercent)
    } else {
      preSampleBootstrapDf
    }

    bootstrapDf.save(bootstrapTable)
    val partitionRange = bootstrapDf.partitionRange

    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", columnName),
        startPartition = partitionRange.start,
        endPartition = partitionRange.end
      ),
      table = bootstrapTable
    )

    (bootstrapPart, bootstrapDf)
  }

  @Test
  def testBootstrap(): Unit = {

    val namespace = "test_table_bootstrap"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    // group by
    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)

    // query
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)

    // Define base join which uses standard backfill
    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    // Runs through standard backfill
    val runner1 = new ai.chronon.spark.Join(baseJoin, today, tableUtils)
    val baseOutput = runner1.computeJoin()

    // Create two bootstrap parts to verify that bootstrap coalesce respects the ordering of the input bootstrap parts
    val (bootstrapTable1, bootstrapTable2) = ("user_transactions_bootstrap1", "user_transactions_bootstrap2")
    val (bootstrapPart1, bootstrapDf1) = buildBootstrapPart(queryTable, namespace, bootstrapTable1)
    val (bootstrapPart2, bootstrapDf2) = buildBootstrapPart(queryTable, namespace, bootstrapTable2)

    // Create bootstrap join using base join as template
    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.user_transaction_features.bootstrap")
    bootstrapJoin.setBootstrapParts(Seq(bootstrapPart1, bootstrapPart2).toJava)

    // Runs through boostrap backfill which combines backfill and bootstrap
    val runner2 = new ai.chronon.spark.Join(bootstrapJoin, today, tableUtils)
    val computed = runner2.computeJoin()

    // Comparison
    val expected = baseOutput
      .join(bootstrapDf1,
            baseOutput("request_id") <=> bootstrapDf1("request_id") and baseOutput("ds") <=> bootstrapDf1("ds"),
            "left")
      .join(bootstrapDf2,
            baseOutput("request_id") <=> bootstrapDf2("request_id") and baseOutput("ds") <=> bootstrapDf2("ds"),
            "left")
      .select(
        baseOutput("user"),
        baseOutput("request_id"),
        baseOutput("ts"),
        coalesce(
          coalesce(bootstrapDf1("unit_test_user_transactions_amount_dollars_sum_30d"),
                   bootstrapDf2("unit_test_user_transactions_amount_dollars_sum_30d")),
          baseOutput("unit_test_user_transactions_amount_dollars_sum_30d")
        ).as("unit_test_user_transactions_amount_dollars_sum_30d"),
        baseOutput("unit_test_user_transactions_amount_dollars_sum_15d"), // not covered by bootstrap
        baseOutput("ds")
      )

    val overlapBaseBootstrap1 = baseOutput.join(bootstrapDf1, Seq("request_id", "ds")).count()
    val overlapBaseBootstrap2 = baseOutput.join(bootstrapDf2, Seq("request_id", "ds")).count()
    val overlapBootstrap12 = bootstrapDf1.join(bootstrapDf2, Seq("request_id", "ds")).count()
    logger.info(s"""Debug information:
         |base count: ${baseOutput.count()}
         |overlap keys between base and bootstrap1 count: ${overlapBaseBootstrap1}
         |overlap keys between base and bootstrap2 count: ${overlapBaseBootstrap2}
         |overlap keys between bootstrap1 and bootstrap2 count: ${overlapBootstrap12}
         |""".stripMargin)

    compareDfs(computed, expected, List("request_id", "user", "ts", "ds"))
  }

  @Test
  def testBootstrapSameJoinPartMultipleSources(): Unit = {

    val namespace = "test_bootstrap_multi_source"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val endDs = spark.table(queryTable).select(max(tableUtils.partitionColumn)).head().getString(0)

    val joinPart = Builders.JoinPart(groupBy = BootstrapUtils.buildGroupBy(namespace, spark))
    val derivations = Seq(
      Builders.Derivation(
        name = "amount_dollars_sum_15d",
        expression = "unit_test_user_transactions_amount_dollars_sum_15d"
      ),
      Builders.Derivation(
        name = "amount_dollars_sum_30d",
        expression = "unit_test_user_transactions_amount_dollars_sum_30d"
      )
    )

    val bootstrapPart1 = buildBootstrapPart(queryTable,
                                            namespace,
                                            tableName = "bootstrap_1",
                                            columnName = "unit_test_user_transactions_amount_dollars_sum_30d",
                                            samplePercent = 1.0)

    val bootstrapPart2 = buildBootstrapPart(queryTable,
                                            namespace,
                                            tableName = "bootstrap_2",
                                            columnName = "unit_test_user_transactions_amount_dollars_sum_15d",
                                            samplePercent = 1.0)
    val join = Builders.Join(
      left = Builders.Source.events(table = queryTable, query = Builders.Query()),
      joinParts = Seq(joinPart),
      rowIds = Seq("request_id"),
      bootstrapParts = Seq(
        bootstrapPart1._1,
        bootstrapPart2._1
      ),
      derivations = derivations,
      metaData = Builders.MetaData(name = "test.bootstrap_multi_source", namespace = namespace, team = "chronon")
    )

    val joinJob = new ai.chronon.spark.Join(join, endDs, tableUtils)
    joinJob.computeJoin()

    // assert that no computation happened for join part since all derivations have been bootstrapped
    assertFalse(tableUtils.tableExists(join.partOutputTable(joinPart)))
  }

  @Test
  def testBootstrapExternalParts(): Unit = {
    val namespace = "test_table_bootstrap_external_parts"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val queriesDf = spark.table(queryTable)
    val externalSourceTable = BootstrapUtils.buildExternalSource(namespace, spark)
    val externalPartDf = spark.table(externalSourceTable)
    val (joinBootstrapPart, joinBootstrapDf) = buildBootstrapPart(queryTable,
      namespace,
      tableName = "join_part_bootstrap",
      columnName = "unit_test_user_transactions_amount_dollars_sum_15d",
      samplePercent = 1.0)

    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = BootstrapUtils.buildGroupBy(namespace, spark))),
      externalParts = Seq(Builders.ExternalPart(Builders.ExternalSource(
        metadata = Builders.MetaData(
          name = "request_source"
        ),
        keySchema = StructType("keys", Array(StructField("request_id", StringType))),
        valueSchema = StructType(
          "values", Array(StructField("external_field1", IntType), StructField("external_field2", IntType)))))),
      derivations =  Seq(
        Builders.Derivation(
          name = "amount_dollars_sum_15d",
          expression = "unit_test_user_transactions_amount_dollars_sum_15d"
        ),
        Builders.Derivation(
          name = "external_field1",
          expression = "ext_request_source_external_field1"
        )
      ),
      rowIds = Seq("request_id"),
      bootstrapParts = Seq(joinBootstrapPart, Builders.BootstrapPart(table = externalSourceTable)),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(baseJoin, today, tableUtils)
    val computed = runner.computeJoin()

    val expected = queriesDf
      .join(joinBootstrapDf,
        queriesDf("request_id") <=> joinBootstrapDf("request_id") and queriesDf("ds") <=> joinBootstrapDf("ds"),
        "left")
      .join(externalPartDf,
        queriesDf("request_id") <=> externalPartDf("request_id") and queriesDf("ds") <=> externalPartDf("ds"),
        "left")
      .select(
        queriesDf("user"),
        queriesDf("request_id"),
        queriesDf("ts"),
        queriesDf("ds"),
        joinBootstrapDf("unit_test_user_transactions_amount_dollars_sum_15d").as("amount_dollars_sum_15d"),
        externalPartDf("ext_request_source_external_field1").as("external_field1"),
      )
    compareDfs(computed, expected, List("request_id", "user", "ts", "ds"))
    val bootstrapDf = spark.table(baseJoin.metaData.bootstrapTable)
    val expectedBootstrapTableColumns = Seq(
      "request_id",
      "user",
      "ds",
      "ts",
      "ts_ds",
      "matched_hashes",
      "unit_test_user_transactions_amount_dollars_sum_15d",
      "ext_request_source_external_field1",
      "ext_request_source_external_field2").sorted
    assertEquals(expectedBootstrapTableColumns, bootstrapDf.columns.toSeq.sorted)
  }

  @Test
  def testSplitExternalPartBootstrap(): Unit = {
    // Define a separate Spark Session for this test to modify its config without affecting other tests
    val spark: SparkSession = SparkSessionBuilder.build(
      "SplitExternalPartBootstrapTest",
      local = true,
      additionalConfig = Some(Map(SparkConstants.ChrononSplitExternalPartsBootstrap -> "true")))
    val namespace = "test_table_split_bootstrap_external_parts"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val queriesDf = spark.table(queryTable)
    val externalSourceTable = BootstrapUtils.buildExternalSource(namespace, spark)
    val externalPartDf = spark.table(externalSourceTable)
    val (joinBootstrapPart, joinBootstrapDf) = buildBootstrapPart(queryTable,
      namespace,
      tableName = "join_part_bootstrap",
      columnName = "unit_test_user_transactions_amount_dollars_sum_15d",
      samplePercent = 1.0)

    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = BootstrapUtils.buildGroupBy(namespace, spark))),
      externalParts = Seq(Builders.ExternalPart(Builders.ExternalSource(
        metadata = Builders.MetaData(
          name = "request_source"
        ),
        keySchema = StructType("keys", Array(StructField("request_id", StringType))),
        valueSchema = StructType(
          "values", Array(StructField("external_field1", IntType), StructField("external_field2", IntType)))))),
      derivations =  Seq(
        Builders.Derivation(
          name = "amount_dollars_sum_15d",
          expression = "unit_test_user_transactions_amount_dollars_sum_15d"
        ),
        Builders.Derivation(
          name = "external_field1",
          expression = "ext_request_source_external_field1"
        )
      ),
      rowIds = Seq("request_id"),
      bootstrapParts = Seq(joinBootstrapPart, Builders.BootstrapPart(table = externalSourceTable)),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(baseJoin, today, tableUtils)
    val computed = runner.computeJoin()

    val expected = queriesDf
      .join(joinBootstrapDf,
        queriesDf("request_id") <=> joinBootstrapDf("request_id") and queriesDf("ds") <=> joinBootstrapDf("ds"),
        "left")
      .join(externalPartDf,
        queriesDf("request_id") <=> externalPartDf("request_id") and queriesDf("ds") <=> externalPartDf("ds"),
        "left")
      .select(
        queriesDf("user"),
        queriesDf("request_id"),
        queriesDf("ts"),
        queriesDf("ds"),
        joinBootstrapDf("unit_test_user_transactions_amount_dollars_sum_15d").as("amount_dollars_sum_15d"),
        externalPartDf("ext_request_source_external_field1").as("external_field1"),
      )
    compareDfs(computed, expected, List("request_id", "user", "ts", "ds"))
    val bootstrapDf = spark.table(baseJoin.metaData.bootstrapTable)
    // The bootstrap table that contained only external parts should have been processed after joins.
    // In that case, bootstrapping happens directly with the source table and should not have been
    // materialized to the intermediate bootstrap table. So we exclude those fields from this list.
    val expectedBootstrapTableColumns = Seq(
      "request_id",
      "user",
      "ds",
      "ts",
      "ts_ds",
      "matched_hashes",
      "unit_test_user_transactions_amount_dollars_sum_15d").sorted
    assertEquals(expectedBootstrapTableColumns, bootstrapDf.columns.toSeq.sorted)
  }

  private def compareDfs(actual: DataFrame, expected: DataFrame, keys: List[String]): Unit = {
    val diff = Comparison.sideBySide(actual, expected, keys)
    if (diff.count() > 0) {
      println(s"Actual count: ${actual.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }
}
