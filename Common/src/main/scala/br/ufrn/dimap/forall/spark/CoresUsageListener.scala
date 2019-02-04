package br.ufrn.dimap.forall.spark

import com.groupon.sparklint._
import com.groupon.sparklint.common.SparklintConfig
import com.groupon.sparklint.common.SparkConfSparklintConfig
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerEvent
import com.groupon.sparklint.analyzer.SparklintStateAnalyzer
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.json4s.DefaultFormats
import java.io.File
import com.groupon.sparklint.events.EventSource
import com.github.tototoshi.csv.CSVWriter

class CoresUsageListener(appId: String, appName: String, config: SparklintConfig) extends SparklintListener(appId, appName, config) {

  var sparkConf: SparkConf = null

  val performanceReportPosFileNameCSV = "-cores-usage-report.csv"

  var performanceReportConfProfile = "conf1"
  var performanceReportType = "print"
  var isSavePerformanceReport = false
  var performanceReportersFolder = ""

  def this(conf: SparkConf) = {
    this(conf.get("spark.app.id", "AppId"), conf.get("spark.app.name", "AppName"), new SparkConfSparklintConfig(conf))
    sparkConf = conf
  }

  override def onEvent(event: SparkListenerEvent): Unit = {
    liveEventSource.onEvent(event)
    event match {
      case e1: SparkListenerApplicationStart => {
        println("CHEGOU AO INICIO DA APLICAÇÃO")
        if (sparkConf.contains("spark.performance.metrics.reports.configurationProfile")) {
          performanceReportConfProfile = sparkConf.get("spark.performance.metrics.reports.configurationProfile")
        }
        if (sparkConf.contains("spark.performance.metrics.reports.type")) {
          performanceReportType = sparkConf.get("spark.performance.metrics.reports.type")
        }
        isSavePerformanceReport = sparkConf.contains("spark.performance.metrics.reports.folder")
        if (isSavePerformanceReport)
          performanceReportersFolder = sparkConf.get("spark.performance.metrics.reports.folder")
      }

      case e2: SparkListenerApplicationEnd => {
        println("CHEGOU AO FIM DA APLICAÇÃO")
        sparklint.backend.listEventSourceGroupManagers.foreach { ev =>
          ev.eventSources.foreach { eventSource =>
            val report = new SparklintStateAnalyzer(eventSource.appMeta, eventSource.appState)
            createReportCSV(eventSource, report)
          }
        }
      } 
      case _ => {}
    }
  }

  def createReportCSV(source: EventSource, report: SparklintStateAnalyzer) = {
    implicit val formats = DefaultFormats
    val meta = report.meta

    val folder = new File(performanceReportersFolder)
    val folderCSV = new File(performanceReportersFolder + File.separator + "csv")
    if (folder.exists() && folder.isDirectory() && folderCSV.exists() && folderCSV.isDirectory()) {

      val reportFileName = meta.appName + "-" + meta.appId + performanceReportPosFileNameCSV
      val reportFile = new File(performanceReportersFolder + File.separator + "csv" + File.separator + reportFileName)
      val writer = CSVWriter.open(reportFile)

      val appId = meta.appId
      val appName = meta.appName
      val appAttemptId = meta.attempt
      val allocatedCores = report.getExecutorInfo.map(_.values.map(_.cores).sum)
      val currentCores = report.getCurrentCores
      val runningTasks = report.getRunningTasks
      val timeUntilFirstTask = report.getTimeUntilFirstTask
      val idleTime = report.getIdleTime
      val idleTimeSinceFirstTask = report.getIdleTimeSinceFirstTask
      val maxConcurrentTasks = report.getMaxConcurrentTasks
      val maxAllocatedCores = report.getMaxAllocatedCores
      val maxCoreUsage = report.getMaxCoreUsage
      val coreUtilizationPercentage = report.getCoreUtilizationPercentage
      val lastUpdatedAt = report.getLastUpdatedAt
      val applicationLaunchedAt = meta.startTime
      val applicationEndedAt = meta.endTime

      val headersRow = List("appId",
        "appName",
        "appAttemptId",
        "allocatedCores",
        "currentCores",
        "runningTasks",
        "timeUntilFirstTask",
        "idleTime",
        "idleTimeSinceFirstTask",
        "maxConcurrentTasks",
        "maxAllocatedCores",
        "maxCoreUsage",
        "coreUtilizationPercentage",
        "lastUpdatedAt",
        "applicationLaunchedAt",
        "applicationEndedAt")

      val valuesRow = List(appId.getOrElse("app-without-id"),
        appName,
        appAttemptId.getOrElse(""),
        allocatedCores.getOrElse(0),
        currentCores.getOrElse(0),
        runningTasks.getOrElse(0),
        timeUntilFirstTask.getOrElse(0),
        idleTime.getOrElse(0),
        idleTimeSinceFirstTask.getOrElse(0),
        maxConcurrentTasks.getOrElse(0),
        maxAllocatedCores.getOrElse(0),
        maxCoreUsage.getOrElse(0),
        coreUtilizationPercentage.getOrElse(0.0),
        lastUpdatedAt.getOrElse(0),
        applicationLaunchedAt,
        applicationEndedAt.getOrElse(0))

      writer.writeRow(headersRow)
      writer.writeRow(valuesRow)
      writer.close()
      println("CSV Cores Usage Report Saved: " + reportFile.getAbsolutePath)
    } else {
      println("CSV Cores Usage Report Not Saved")
      println("Invalid Performance Reports Folder: " + performanceReportersFolder)
    }
  }

}