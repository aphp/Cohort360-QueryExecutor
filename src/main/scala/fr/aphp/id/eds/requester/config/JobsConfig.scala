package fr.aphp.id.eds.requester.config

import fr.aphp.id.eds.requester.{CountQuery, CreateQuery}

object JobsConfig {
  val countJob: CountQuery = new CountQuery
  val createJob: CreateQuery = new CreateQuery
}
