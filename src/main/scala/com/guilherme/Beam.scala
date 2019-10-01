package com.guilherme

import java.io.FileInputStream

import com.typesafe.scalalogging.LazyLogging
import com.google.api.services.bigquery.model.{ TableReference, TableRow }
import com.google.api.services.dataflow.DataflowScopes
import com.google.auth.oauth2.ServiceAccountCredentials
import org.apache.beam.runners.dataflow.DataflowRunner
//import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.{ PipelineOptions, PipelineOptionsFactory }
import org.apache.beam.sdk.transforms.{ DoFn, ParDo }
import DoFn.ProcessElement
import org.apache.beam.runners.direct.DirectRunner
//import org.apache.beam.sdk.coders.{ AvroCoder, DefaultCoder }
import org.apache.beam.sdk.util.Transport
import scala.util.{ Failure, Success, Try }

object Beam extends App {

  /* credencial  gerada no gcp */
  val keyLocation = "chave.json"

  /* parametros do pipeline */
  val projectId = "data-devel-254113"
  val gcpTempLocation = "gs://guilherme_moreira/temp"
  val dataset = "test_dataset"
  val tableName = "test"
  val subscription = "test-dataflow"

  /* config para o runner */
  val runner = classOf[DataflowRunner]
  //val runner = classOf[DirectRunner]
  val numWorkers = 1
  val zone = "us-west1-a"
  val workerMachineType = "n1-standard-1"

  /* Cria o objeto option para iniciar o pipeline */
  trait TestOptions extends PipelineOptions with DataflowPipelineOptions
  val options = PipelineOptionsFactory.create().as(classOf[TestOptions])

  options.setGcpCredential(ServiceAccountCredentials.fromStream(new FileInputStream(keyLocation)).createScoped(DataflowScopes.all))
  options.setProject(projectId)
  options.setRunner(runner)
  options.setNumWorkers(numWorkers)
  options.setZone(zone)
  options.setWorkerMachineType(workerMachineType)
  options.setGcpTempLocation(gcpTempLocation)
  options.setStreaming(true)

  /* Pipeline le a mensagem do input e faz streaming direto no BigQuery  */
  val fullSubscriptionName = s"projects/$projectId/subscriptions/$subscription"
  val targetTable = new TableReference().setProjectId(projectId).setDatasetId(dataset).setTableId(tableName)

  /* Tentando converter Strings para TableRow */
  class MyDoFn extends DoFn[String, TableRow] with LazyLogging {
    @ProcessElement
    def processElement(c: ProcessContext) {
      val inputString = c.element()
      logger.info(s"Mensagem recebida: $inputString")
      Try {
        Transport.getJsonFactory.fromString(inputString, classOf[TableRow])
      } match {
        case Success(row) ⇒
          logger.info(s"Convertido TableRow: $row")
          c.output(row)
        case Failure(ex) ⇒
          logger.info(s"Erro: $inputString", ex)
      }
    }
  }

  /* construção de pipeline para ler Strings do PubsubIO, convertê-las em TableRows e gravar na tabela BQ, não validando o esquema da tabela */
  val p = Pipeline.create(options)
  p.apply("read-pubsub", PubsubIO.readStrings().fromSubscription(fullSubscriptionName))
    .apply("process", ParDo.of(new MyDoFn))
    .apply("write-bq", BigQueryIO
      .writeTableRows()
      .to(targetTable)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER))

  /* inicia pipeline */
  p.run()

}