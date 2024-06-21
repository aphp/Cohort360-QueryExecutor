package fr.aphp.id.eds.requester.tools

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import fr.aphp.id.eds.requester.AppConfig
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpException}


object HttpTools extends LazyLogging {

  private val httpClient = HttpClientBuilder.create().build()

  def httpPatchRequest(url: String, data: AnyRef): String = {
    httpPatchRequest(url, getBasicBearerTokenHeader(AppConfig.get.back.authToken), data)
  }

  def httpPatchRequest(url: String, headerConfig: Map[String, String], data: AnyRef): String = {
    logger.info(s"PATCH request on URL $url")
    val request = new HttpPatch(url)
    val body = new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .writeValueAsString(data)
    processResponse(request, headerConfig, body)
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

  def getBasicBearerTokenHeader(token: Option[String]): Map[String, String] = {
    val defaultHeaders = Map("Content-Type" -> "application/json")
    if (token.isEmpty) {
      return defaultHeaders
    }
    defaultHeaders ++ Map("Authorization" -> s"Bearer ${token.get}")
  }

}
