package fr.aphp.id.eds.requester.query.model

import fr.aphp.id.eds.requester.tools.StageDetails
import org.apache.spark.sql.SparkSession

case class CacheConfig(
    ownerEntityId: String,
    enableCurrentGroupCache: Boolean,
    cacheNestedGroup: Boolean
)

case class QueryContext(
    sparkSession: SparkSession,
    sourcePopulation: SourcePopulation,
    stageDetails: StageDetails,
    cacheConfig: CacheConfig
)
