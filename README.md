# OMOP-SPARK-JOB

### Compilation du jar `spark-job-server.jar`
Le projet open-source [spark-job-server](https://github.com/spark-jobserver/spark-jobserver) mis en submodule de ce projet
a été bloqué au commit du 15 Décembre 2020 `e2aeb9b5e73d57590528674f74fdceb1b2c34dce`.

Pour compiler, il faut installer 
[java](https://www.oracle.com/java/technologies/downloads/) (and set JAVA_HOME and add bin to PATH) 
puis [sbt](https://www.scala-sbt.org/download.html).

De plus, il faut modifier la ligne suivante dans le fichier `job-server/src/main/scala/spark/jobserver/BinaryManager.scala`:  
`implicit val daoAskTimeout = Timeout(60 seconds)` => `implicit val daoAskTimeout = Timeout(300 seconds)`

Enfin, pour le compiler et le rendre disponible:
`cd spark-jobserver && sbt job-server-extras/assembly && cd .. && cp spark-jobserver/job-server-extras/target/scala-2.11/spark-job-server.jar docker/sjs/lib/spark-job-server.jar`