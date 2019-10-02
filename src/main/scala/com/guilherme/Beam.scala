package com.guilherme

import java.io.FileInputStream
import java.io.FileInputStream

import com.typesafe.scalalogging.LazyLogging
import com.google.api.services.bigquery.model.{ TableReference, TableRow }
import com.google.api.services.dataflow.DataflowScopes
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.gson.JsonParser
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.PCollectionView
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
  val sideInputSubscription = "test-dataflow-sideinput"

  /* config para o runner */
  //val runner = classOf[DataflowRunner]
  val runner = classOf[DirectRunner]
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
  val fullSideInputSubscriptionName = s"projects/$projectId/subscriptions/$sideInputSubscription"
  val targetTable = new TableReference().setProjectId(projectId).setDatasetId(dataset).setTableId(tableName)

  /* Tentando converter Strings para TableRow */
  class MyDoFn(sideView: PCollectionView[java.util.List[String]]) extends DoFn[String, TableRow] with LazyLogging {
    @ProcessElement
    def processElement(c: ProcessContext) {
      val sideInput = c.sideInput(sideView).get(0)
      val inputString = c.element()
      if (sideInput == "ENABLED") {
        Try {
          Transport.getJsonFactory.fromString(inputString, classOf[TableRow])
        } match {
          case Success(row) ⇒
            logger.info(s"Inserting to BiqQuery: $row")
            c.output(row)
          case Failure(ex) ⇒
            logger.info(s"Unable to parse message: $inputString", ex)
        }
      } else {
        logger.info(s"Ignoring input messages, sideInput=$sideInput")
      }
    }
  }

  /* construção de pipeline para ler Strings do PubsubIO, convertê-las em TableRows e gravar na tabela BQ, não validando o esquema da tabela */
  val p = Pipeline.create(options)

  /* lendo o topico de controle */
  val sideView = p.apply("read-pubsub-side", PubsubIO.readStrings().fromSubscription(fullSideInputSubscriptionName))
    .apply("global_side_input", Window.into[String](new GlobalWindows())
      .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
      .discardingFiredPanes())
    .apply("side_view", View.asList())

  /* final pipeline */
  p.apply("read-pubsub", PubsubIO.readStrings().fromSubscription(fullSubscriptionName))
    .apply("process", ParDo.of(new MyDoFn(sideView)).withSideInputs(sideView))
    .apply("write-bq", BigQueryIO
      .writeTableRows()
      .to(targetTable)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER))

  /* start pipeline */
  p.run()

}
