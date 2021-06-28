package fr.aphp.id.eds.requester.server

import org.scalatra.ScalatraServlet

class HealthController extends ScalatraServlet {

  get("/") {
    "OK"
  }
}
