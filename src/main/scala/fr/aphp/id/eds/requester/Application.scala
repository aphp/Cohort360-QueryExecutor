package fr.aphp.id.eds.requester

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

object Application {

  def main(args: Array[String]): Unit = {
    val port = AppConfig.get.server.port

    val server = new Server(port)
    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    context.addEventListener(new ScalatraListener)
    context.setInitParameter(ScalatraListener.LifeCycleKey,
                             "fr.aphp.id.eds.requester.server.CohortRequestServer")
    context.addServlet(classOf[DefaultServlet], "/")

    server.setHandler(context)

    server.start()
    server.join()
  }
}
