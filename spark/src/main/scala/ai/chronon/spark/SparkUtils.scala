package ai.chronon.spark

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, KeyValueGroupedDataset}

trait SparkUtils extends Serializable {

  def cogroupSorted[K, V, U, R: Encoder](
                                          leftDataset: KeyValueGroupedDataset[K, V],
                                          rightDataset: KeyValueGroupedDataset[K, U],
                                          leftOrdering: Seq[Column],
                                          rightOrdering: Seq[Column],
                                          f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]
                                        ): Dataset[R]

  // Allows implementers to cache the given dataframe using custom caching
  // or conditional logic. Default is a no-op.
  def optionalCache(df: DataFrame): DataFrame = df

}
