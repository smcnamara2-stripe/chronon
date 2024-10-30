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

package ai.chronon.api.test

import ai.chronon.api.{Accuracy, Builders, Constants, GroupBy, Join}
import org.junit.Test
import ai.chronon.api.Extensions._
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.mockito.Mockito.{spy, when}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.`type`.TypeReference

import scala.util.ScalaJavaConversions.JListOps
import java.util.Arrays

class ExtensionsTest {

  @Test
  def testSubPartitionFilters(): Unit = {
    val source = Builders.Source.events(query = null, table = "db.table/system=mobile/currency=USD")
    assertEquals(
      Map("system" -> "mobile", "currency" -> "USD"),
      source.subPartitionFilters
    )
  }

  @Test
  def testOwningTeam(): Unit = {
    val metadata =
      Builders.MetaData(
        customJson = "{\"check_consistency\": true, \"lag\": 0, \"team_override\": \"ml_infra\"}",
        team = "chronon"
      )

    assertEquals(
      "ml_infra",
      metadata.owningTeam
    )

    assertEquals(
      "chronon",
      metadata.team
    )
  }

  @Test
  def testRowIdentifier(): Unit = {
    val labelPart = Builders.LabelPart();
    val res = labelPart.rowIdentifier(Arrays.asList("yoyo", "yujia"), "ds")
    assertTrue(res.contains("ds"))
  }

  @Test
  def partSkewFilterShouldReturnNoneWhenNoSkewKey(): Unit = {
    val joinPart = Builders.JoinPart()
    val join = Builders.Join(joinParts = Seq(joinPart))
    assertTrue(join.partSkewFilter(joinPart).isEmpty)
  }

  @Test
  def partSkewFilterShouldReturnCorrectlyWithSkewKeys(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("a", "c"), metaData = groupByMetadata)
    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))
    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("a NOT IN (b) OR c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnCorrectlyWithPartialSkewKeys(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("c"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnCorrectlyWithSkewKeysWithMapping(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("x", "c"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy, keyMapping = Map("a" -> "x"))
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("x NOT IN (b) OR c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnNoneIfJoinPartHasNoRelatedKeys(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("non_existent"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).isEmpty)
  }

  @Test
  def groupByKeysShouldContainPartitionColumn(): Unit = {
    val groupBy = spy(new GroupBy())
    val baseKeys = List("a", "b")
    val partitionColumn = "ds"
    groupBy.accuracy = Accuracy.SNAPSHOT
    groupBy.keyColumns = baseKeys.toJava
    when(groupBy.isSetKeyColumns).thenReturn(true)

    val keys = groupBy.keys(partitionColumn)
    assertTrue(baseKeys.forall(keys.contains(_)))
    assertTrue(keys.contains(partitionColumn))
    assertEquals(3, keys.size)
  }

  @Test
  def groupByKeysShouldContainTimeColumnForTemporalAccuracy(): Unit = {
    val groupBy = spy(new GroupBy())
    val baseKeys = List("a", "b")
    val partitionColumn = "ds"
    groupBy.accuracy = Accuracy.TEMPORAL
    groupBy.keyColumns = baseKeys.toJava
    when(groupBy.isSetKeyColumns).thenReturn(true)

    val keys = groupBy.keys(partitionColumn)
    assertTrue(baseKeys.forall(keys.contains(_)))
    assertTrue(keys.contains(partitionColumn))
    assertTrue(keys.contains(Constants.TimeColumn))
    assertEquals(4, keys.size)
  }

  @Test
  def testIsTilingEnabled(): Unit = {
    def buildGroupByWithCustomJson(customJson: String = null): GroupBy =
      Builders.GroupBy(
        metaData = Builders.MetaData(name = "featureGroupName", customJson = customJson)
      )

    // customJson not set defaults to false
    assertFalse(buildGroupByWithCustomJson().isTilingEnabled)
    assertFalse(buildGroupByWithCustomJson("{}").isTilingEnabled)

    assertTrue(buildGroupByWithCustomJson("{\"enable_tiling\": true}").isTilingEnabled)
    assertFalse(buildGroupByWithCustomJson("{\"enable_tiling\": false}").isTilingEnabled)
    assertFalse(buildGroupByWithCustomJson("{\"enable_tiling\": \"string instead of bool\"}").isTilingEnabled)
  }

  @Test
  def testOutputTableMapAccess(): Unit = {
    def buildJoinWithCustomJson(customJson: String = null): Join =
      Builders.Join(
        metaData = Builders.MetaData(name = "featureGroupName", customJson = customJson)
      )
    // chronon/config_util/GroupByConfUtil.scala maps "outputs" in the customJson into "output_tables"
    val outputTable = "mocked_table_name"
    val joinWithOutputTableMap = buildJoinWithCustomJson(
      s"""{\"output_tables\": {
        |\"output\":  \"$outputTable\"
        |}}
        |""".stripMargin)

    assertEquals("existing key value look up should be successful",outputTable, joinWithOutputTableMap.metaData.outputTable)

    val bootStrapTableDefaultValue = s"${joinWithOutputTableMap.metaData.outputTable}_bootstrap"
    assertEquals("non-existing key value look up should return the fallback value", bootStrapTableDefaultValue, joinWithOutputTableMap.metaData.bootstrapTable)

    val joinWithNoOutputTableMap = buildJoinWithCustomJson()
    assertEquals("when output map is missing, return the fallback value", s"${joinWithNoOutputTableMap.metaData.outputNamespace}.${joinWithNoOutputTableMap.metaData.cleanName}", joinWithNoOutputTableMap.metaData.outputTable)
  }

  @Test
  def testUpdateCustomJson(): Unit = {
    val metadata = Builders.MetaData(customJson = null)

    // Test adding a new key-value pair to empty customJson
    metadata.updateCustomJson("new_key", "new_value")
    val updatedJson1 = metadata.customJson
    assertTrue(updatedJson1.contains("\"new_key\":\"new_value\""))

    // Test updating an existing key
    metadata.updateCustomJson("new_key", "updated_value")
    val updatedJson2 = metadata.customJson
    assertTrue(updatedJson2.contains("\"new_key\":\"updated_value\""))
    assertFalse(updatedJson2.contains("\"new_key\":\"new_value\""))

    // Test adding a new key-value pair to existing customJson
    metadata.updateCustomJson("another_key", 42)
    val updatedJson3 = metadata.customJson
    assertTrue(updatedJson3.contains("\"new_key\":\"updated_value\""))
    assertTrue(updatedJson3.contains("\"another_key\":42"))

    // Verify the final state of customJson
    val mapper = new ObjectMapper()
    val typeRef = new TypeReference[java.util.HashMap[String, Object]]() {}
    val finalMap: java.util.Map[String, Object] = mapper.readValue(metadata.customJson, typeRef)

    assertEquals("updated_value", finalMap.get("new_key"))
    assertEquals(42, finalMap.get("another_key"))
  }
}
