package fr.aphp.id.eds.requester

import fr.aphp.id.eds.requester.JobUtils.{initSparkJobRequest, getDefaultSolrFilterQuery}
import fr.aphp.id.eds.requester.jobs.{JobBase, JobEnv}
import fr.aphp.id.eds.requester.query.{BasicResource, GroupResource, QueryBuilder}
import fr.aphp.id.eds.requester.tools.SolrTools.getSolrClient
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.SparkSession

object CountQuery extends JobBase{
  type JobData = SparkJobParameter
  type JobOutput = Long

  private val logger = Logger.getLogger(this.getClass)

  override def runJob(spark: SparkSession, runtime: JobEnv, data: JobData): JobOutput = {
    logger.info("[COUNT] New " + data.mode + " asked by " + data.ownerEntityId)
    val (request, criterionTagsMap, solrConf, omopTools, cacheEnabled) = initSparkJobRequest(logger, spark, runtime, data)

    def isGroupResourceAndHasCriteria =
      request.request.get.isInstanceOf[GroupResource] && request.request.get.asInstanceOf[GroupResource].criteria.nonEmpty

    def isInstanceOfBasicResource = request.request.get.isInstanceOf[BasicResource]

    def theRequestHasAtLeastOneCriteria(): Boolean = {
      request.request.isDefined && (isInstanceOfBasicResource || isGroupResourceAndHasCriteria)
    }

    def countPatientsWithSpark() = {
      QueryBuilder
          .processRequest(spark, solrConf, request, criterionTagsMap, omopTools, data.ownerEntityId, cacheEnabled)
          .count()
    }

    def countPatientsInSolr() = {
      val solr = getSolrClient(solrConf("zkhost"))
      val query = new SolrQuery("*:*").addFilterQuery(getDefaultSolrFilterQuery(request.sourcePopulation))
      val res = solr.query(SolrCollection.PATIENT_APHP, query)
      solr.close()
      res.getResults.getNumFound
    }

    def countPatientsInQuery() = {
      if (theRequestHasAtLeastOneCriteria()) countPatientsWithSpark() else countPatientsInSolr()
    }

    if (request.sourcePopulation.caresiteCohortList.isEmpty)
      throw new Exception(
        "INPUT JSON cannot be processed (missing input 'sourcePopulation' and/or 'request')")

    countPatientsInQuery()
  }

//  /** Check that all inputs are defined and have the right format. This method is required by the SJS. */
//  override def validate(sc: SparkSession, runtime: JobEnvironment, config: Config): JobData Or Every[ValidationProblem] = {
//    try {
//      val res: JobData = SparkJobParameter(
//        cohortDefinitionName = config.getString("input.cohortDefinitionName"),
//        cohortDefinitionDescription =
//          Try(Some(config.getString("input.cohortDefinitionDescription")))
//            .getOrElse(None),
//        cohortDefinitionSyntax = config.getString("input.cohortDefinitionSyntax"),
//        ownerEntityId = config.getString("input.ownerEntityId"),
//        mode = config.getString("input.mode"),
//        cohortUuid = None
//      )
//      Good(res)
//    } catch {
//      case e: Exception => Bad(One(SingleProblem(e.getMessage)))
//    }
//  }
}
