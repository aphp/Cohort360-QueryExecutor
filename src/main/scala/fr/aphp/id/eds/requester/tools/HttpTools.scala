package fr.aphp.id.eds.requester.tools

import com.typesafe.scalalogging.LazyLogging
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpException}


object HttpTools extends LazyLogging {

  private val httpClient = HttpClientBuilder.create().build()
  private val token = sys.env.getOrElse("SJS_TOKEN", throw new RuntimeException("No token provided"))

  def httpPatchRequest(url: String, requestBody: String): String = {
    httpPatchRequest(url, getBasicBearerTokenHeader(token), requestBody)
  }

  def httpPatchRequest(url: String, headerConfig: Map[String, String], requestBody: String): String = {
    logger.info(s"PATCH request on URL $url")
    val request = new HttpPatch(url)
    processResponse(request, headerConfig, requestBody)
  }

  /**
   * Sends an HTTP request using the specified `httpClient`, `request`, and `headerConfig`,
   * and returns the response as a string.
   *
   * @param request      The `HttpUriRequest` to send.
   * @param headerConfig A `Map` of header keys to values to add to the request.
   * @param requestBody  A string representing the request body. If provided, it will be added to the request.
   * @return The response from the server, as a string.
   */
  private def processResponse(request: HttpUriRequest,
                      headerConfig: Map[String, String], requestBody: String = null): String = {
    addHeaders(request, headerConfig)
    addJsonBody(request, requestBody)
    val response = httpClient.execute(request)
    val statusCode = response.getStatusLine.getStatusCode
    val responseBody = getEntityString(response.getEntity)
    fetchBadStatusCodeResponse(statusCode, responseBody)
    logger.info("response body " + responseBody)
    responseBody
  }

  private def getEntityString(response: HttpEntity): String = {
    if (response == null) {
      return "{}"
    }
    EntityUtils.toString(response)
  }

  private def addHeaders(request: HttpUriRequest, headerConfig: Map[String, String]): Unit = {
    headerConfig.foreach { case (name, value) => request.addHeader(name, value) }
  }

  private def addJsonBody(request: HttpUriRequest, jsonBody: String): Unit = {
    if (jsonBody == null)
      return
    request match {
      case r: HttpPost => r.setEntity(new StringEntity(jsonBody))
      case r: HttpPatch => r.setEntity(new StringEntity(jsonBody))
    }
  }

  private def fetchBadStatusCodeResponse(statusCode: Int, response: String): Unit = {
    if (statusCode < 200 || statusCode >= 300) {
      throw new HttpException(s"Request failed with status code: $statusCode and reason: $response")
    }
  }

  def getBasicBearerTokenHeader(token: String): Map[String, String] = {
    Map("Content-Type" -> "application/json", "Authorization" -> s"Bearer $token")
  }

}
