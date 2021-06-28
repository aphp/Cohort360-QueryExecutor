package fr.aphp.id.eds.requester.server

import org.scalatra.ScalatraServlet
import org.scalatra.swagger.{ApiInfo, ContactInfo, LicenseInfo, NativeSwaggerBase, Swagger}


class SwaggerController(implicit val swagger: Swagger) extends ScalatraServlet with NativeSwaggerBase

object CohortRequesterApiInfo extends ApiInfo(
  "Cohort Requester API",
  "Docs for the Cohort Requester API",
  "https://aphp.fr",
  ContactInfo("APHP","https://aphp.fr", "dsi-id-recherche-support-cohort360@aphp.fr"),
  LicenseInfo("MIT", "http://opensource.org/licenses/MIT")
  )

class CohortRequesterSwagger extends Swagger(Swagger.SpecVersion, "1.0.0", CohortRequesterApiInfo)
