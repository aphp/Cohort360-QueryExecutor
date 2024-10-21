package fr.aphp.id.eds.requester.query.model

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

case class CacheConfig(
    ownerEntityId: String,
    enableCurrentGroupCache: Boolean,
    cacheNestedGroup: Boolean
)

case class QueryContext(
    sparkSession: SparkSession,
    sourcePopulation: SourcePopulation,
    sourcePopulationCount: Long,
    stageCounts: Option[mutable.Map[Short, Long]],
    cacheConfig: CacheConfig
)
