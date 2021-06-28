package fr.aphp.id.eds.requester.jobs

case class JobStatus(status: String,
                     jobId: String,
                     context: String,
                     startTime: String,
                     duration: String,
                     result: AnyRef,
                     classPath: String) {

}
