package fr.aphp.id.eds.requester.tools

import com.typesafe.scalalogging.LazyLogging
import fr.aphp.id.eds.requester.SolrConfig
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpSolrClient}
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.compat.java8.OptionConverters.RichOptionForJava8

class SolrTools(solrConfig: SolrConfig) extends LazyLogging {

  /** Read SolR passthrough parameters in SJS conf file */
  def getSolrConf: Map[String, String] = {
    val zkHost = solrConfig.zk
    val maxSolrTry = solrConfig.maxTry
    val rows = solrConfig.rows
    val commitWithin = solrConfig.commitWithin

    val options = Map(
      "zkhost" -> zkHost,
      "batch_size" -> "10000",
      "timezone_id" -> "Europe/Paris",
      "request_handler" -> "/export",
      "flatten_multivalued" -> "false",
      "rows" -> rows.toString,
      "commit_within" -> commitWithin.toString,
      "max_solr_try" -> maxSolrTry.toString
    )
    options
  }

  def getSolrClient: CloudSolrClient = {
    import scala.collection.JavaConverters._
    val zkHostList = solrConfig.zk.split(",").toList.asJava
    new CloudSolrClient.Builder(zkHostList, (None: Option[String]).asJava)
      .build()
  }

  /**
    * Check cohort creation
    */
  def checkReplications(cohortDefinitionId: Long, cohortCount: Long): Unit = {
    def validateReplications(groupId: Long, urls: List[String]): Boolean = {
      val counts: List[Long] = urls.map(url => {
        val solrServer = new HttpSolrClient.Builder(url).build
        val q = new SolrQuery("*:*").addFilterQuery(s"groupId:$groupId")
        q.setRows(0)
        val count: Long =
          solrServer.query("groupAphp", q).getResults.getNumFound
        count
      })
      counts.foreach(count => logger.debug("[checking cohort creation] count i = %s".format(count)))
      val isRunning: Boolean = counts.exists(count => count != cohortCount)
      logger.debug(
        "[checking cohort creation] Is replications are still running : %s"
          .format(isRunning))
      isRunning
    }

    logger.debug("[checking cohort creation] originalCounter = %s".format(cohortCount))
    if (cohortCount != 0) {
      val client = getSolrClient
      val request = new CollectionAdminRequest.ClusterStatus()
      val response = client.request(request)

      val str = response.jsonStr
      val js = Json.parse(str)
      val collections = (js \ "cluster" \ "collections").get
      val grp = collections.asInstanceOf[JsObject].value("groupAphp")
      val shards =
        grp.asInstanceOf[JsObject].value("shards").asInstanceOf[JsObject].value

      val urlsAliveNode: List[String] = shards
        .flatMap(
          shard =>
            shard._2
              .asInstanceOf[JsObject]
              .value("replicas")
              .asInstanceOf[JsObject]
              .value
              .filter(
                replicat =>
                  replicat._2
                    .asInstanceOf[JsObject]
                    .value("state")
                    .toString
                    .replaceAll("^\"|\"$", "")
                    .equals("active"))
              .map(replicat => {
                replicat._2
                  .asInstanceOf[JsObject]
                  .value("base_url")
                  .toString
                  .replaceAll("^\"|\"$", "")
              }))
        .toList
        .distinct

      warnDownNodes(shards)

      var isRunning: Boolean = true
      var callNumber: Int = 0

      while (isRunning) {
        Thread.sleep(5000)
        isRunning = validateReplications(cohortDefinitionId, urlsAliveNode)
        logger.debug("[checking cohort creation], call number = %d".format(callNumber))
        callNumber += 1
      }
      client.close()
    }
  }

  private def warnDownNodes(shards: scala.collection.Map[String, JsValue]): Unit = {
    val urlsNodeDown: List[String] = shards
      .flatMap(
        shard =>
          shard._2
            .asInstanceOf[JsObject]
            .value("replicas")
            .asInstanceOf[JsObject]
            .value
            .filter(
              replicat =>
                !replicat._2
                  .asInstanceOf[JsObject]
                  .value("state")
                  .toString
                  .replaceAll("^\"|\"$", "")
                  .equals("active"))
            .map(replicat => {
              replicat._2
                .asInstanceOf[JsObject]
                .value("base_url")
                .toString
                .replaceAll("^\"|\"$", "")
            }))
      .toList
      .distinct

    if (urlsNodeDown.nonEmpty) {
      logger.warn(
        s"[checking cohort creation] **** following SolR nodes are note available: $urlsNodeDown")
    }
  }

}
