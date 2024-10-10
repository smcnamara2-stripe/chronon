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

package ai.chronon.spark

import org.slf4j.LoggerFactory
import ai.chronon.online.Extensions.StructTypeOps
import com.google.gson.Gson
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, MapType}

import java.util

object Comparison {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  // used for comparison
  def sortedJson(m: Map[String, Any]): String = {
    if (m == null) return null
    val tm = new util.TreeMap[String, Any]()
    m.iterator.foreach { case (key, value) => tm.put(key, value) }
    val gson = new Gson()
    gson.toJson(tm)
  }

  def stringifyMaps(df: DataFrame): DataFrame = {
    try {
      df.sparkSession.udf.register("sorted_json", (m: Map[String, Any]) => sortedJson(m))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val selects = for (field <- df.schema.fields) yield {
      if (field.dataType.isInstanceOf[MapType]) {
        s"sorted_json(${field.name}) as `${field.name}`"
      } else {
        s"${field.name} as `${field.name}`"
      }
    }
    df.selectExpr(selects: _*)
  }

  /**
   * Produces a "comparison" dataframe - given two dataframes that are supposed to have same data.
   * The result contains the differing rows of the same key.
   * @param a first dataframe to compare
   * @param b second dataframe to compare
   * @param keys dataframe keys to use for comparison
   * @param aName name that identifies the first dataframe
   * @param bName name that identifies the second dataframe
   * @param doubleTolerance tolerance for comparing double values (a.k.a. epsilon)
   * @param includeDiffCols if true, the resulting comparison dataframe will include a "_diff" column for each column
   *                        being compared, specifying if the column value differs between the two dataframes
   */
  def sideBySide(a: DataFrame,
                 b: DataFrame,
                 keys: List[String],
                 aName: String = "a",
                 bName: String = "b",
                 doubleTolerance: Double = 0.00001,
                 includeColumnDiffs: Boolean = false): DataFrame = {

    logger.info(
      s"""
        |====== side-by-side comparison ======
        |keys: $keys\na_schema:\n${a.schema.pretty}\nb_schema:\n${b.schema.pretty}
        |""".stripMargin
    )

    val prefixedExpectedDf = prefixColumnName(stringifyMaps(a), s"${aName}_")
    val prefixedOutputDf = prefixColumnName(stringifyMaps(b), s"${bName}_")

    val joinExpr = keys
      .map(key => prefixedExpectedDf(s"${aName}_$key") <=> prefixedOutputDf(s"${bName}_$key"))
      .reduce((col1, col2) => col1.and(col2))
    val joined = prefixedExpectedDf.join(
      prefixedOutputDf,
      joinExpr,
      joinType = "full_outer"
    )

    var finalDf = joined
    val comparisonColumns =
      a.schema.fieldNames.toSet.diff(keys.toSet).toList.sorted
    // double columns need to be compared approximately
    val doubleCols = a.schema.fields
      .filter(field =>
        field.dataType == DoubleType || field.dataType == FloatType || field.dataType.isInstanceOf[DecimalType])
      .map(_.name)
      .toSet
    val comparisonFilters = comparisonColumns
      .map { col => col -> {
        val left = s"${aName}_$col"
        val right = s"${bName}_$col"
        val compareExpression =
          if (doubleCols.contains(col)) {
            val definedNotEqual = s"${isDefinedDouble(left)} AND ${isDefinedDouble(right)} AND abs($left - $right) > $doubleTolerance"
            val undefinedNotEqual = s"${isUndefinedDouble(left)} AND $left <> $right"
            s"(($definedNotEqual) OR ($undefinedNotEqual))"
          } else s"($left <> $right)"
        s"(($left IS NULL AND $right IS NOT NULL) OR ($right IS NULL AND $left IS NOT NULL) OR $compareExpression)"
      }
    }
    val comparisonMap = comparisonFilters.toMap
    val colOrder =
      keys.map(key => { finalDf(s"${aName}_$key").as(key) }) ++
        comparisonColumns.flatMap { col => {
          val cols = List(finalDf(s"${aName}_$col"), finalDf(s"${bName}_$col"))
          if (includeColumnDiffs) cols :+ expr(s"${comparisonMap(col)}").as(s"${col}_diff") else cols
        }
      }
    finalDf = finalDf.select(colOrder: _*)
    logger.info(s"Using comparison filter:\n  ${comparisonFilters.map(_._2).mkString("\n  ")}")
    if (comparisonFilters.nonEmpty) finalDf.filter(comparisonFilters.map(_._2).mkString(" OR ")) else {
      // all rows are good
      finalDf.filter("false")
    }
  }

  private def isDefinedDouble(value: String): String = {
    s"$value IS NOT NULL AND $value NOT IN (double('infinity'), double('-infinity'), double('NaN'))"
  }

  private def isUndefinedDouble(value: String): String = {
    s"$value IN (double('infinity'), double('-infinity'), double('NaN'))"
  }

  private def prefixColumnName(df: DataFrame, prefix: String) = {
    val renamedColumns = df.columns.map(c => { df(c).as(s"$prefix$c") })
    df.select(renamedColumns: _*)
  }
}
