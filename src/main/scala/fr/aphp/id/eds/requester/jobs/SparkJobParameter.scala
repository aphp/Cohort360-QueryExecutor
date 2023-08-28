package fr.aphp.id.eds.requester.jobs

case class SparkJobParameter(
    cohortDefinitionName: String,
    cohortDefinitionDescription: Option[String],
    cohortDefinitionSyntax: String,
    ownerEntityId: String,
    solrRows: String = "10000",
    commitWithin: String = "10000",
    mode: String = "count",
    cohortUuid: Option[String] = Option.empty,
    callbackUrl: Option[String] = Option.empty
)
