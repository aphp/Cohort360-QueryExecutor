package fr.aphp.id.eds.requester.server

import org.scalatra.LifeCycle

import javax.servlet.ServletContext

class CohortRequestServer extends LifeCycle {
  implicit val swagger: CohortRequesterSwagger = new CohortRequesterSwagger

  override def init(context: ServletContext): Unit = {
    context mount (new HealthController, "/")
    context mount (new JobController, "/jobs")
    context.mount(new SwaggerController, "/api-docs")
  }
}
