package fr.aphp.id.eds.requester.jobs

import org.json4s.FieldSerializer
import org.json4s.FieldSerializer.{renameFrom, renameTo}

object JobResultSerializer {
  val serializer: FieldSerializer[JobResult] = FieldSerializer[JobResult](
    renameTo("groupId", "group.id"),
    renameFrom("group.id", "groupId")
  )
}

case class JobResult(_type: String,
                     message: String,
                     groupId: String = "",
                     count: Long = -1,
                     stack: String = "",
                     source: String = "")
