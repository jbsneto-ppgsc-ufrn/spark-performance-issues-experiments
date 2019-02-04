package br.ufrn.dimap.forall.spark

import java.io.File
import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.storage.RDDInfo

import com.github.tototoshi.csv.CSVWriter
import com.google.gson.GsonBuilder

import br.ufrn.dimap.forall.spark.MetricsUtils.formatBytes
import br.ufrn.dimap.forall.spark.MetricsUtils.formatDuration

class PerformanceMetricsListener(conf: SparkConf) extends SparkListener {

  case class ApplicationPerformanceMetrics(
    appId: String,
    appName: String,
    configurationProfile: String,
    initialTime: Long,
    finalTime: Long,
    applicationDuration: Long,
    numJobs: Int,
    numFailedJobs: Int,
    stagesPerformanceMetrics: AggragatedStagesPerformanceMetrics)

  case class AggragatedStagesPerformanceMetrics(
    numStages: Int,
    numFailedStages: Int,
    totalStagesElapsedTime: Long,
    totalStagesDuration: Long,
    tasksPerformanceMetrics: AggregatedTasksPerformanceMetrics,
    rddsPerformanceMetrics: AggregatedRDDsPerformanceMetrics)

  case class AggregatedTasksPerformanceMetrics(
    numTasks: Int,
    numSuccessfulTasks: Int,
    numFailedTasks: Int,
    numKilledTasks: Int,
    totalExecutionRunTime: Long,
    totalExecutionCpuTime: Long,
    totalExecutorDeserializeTime: Long,
    totalExecutorDeserializeCpuTime: Long,
    totalResultSerializationTime: Long,
    totalJvmGCTime: Long,
    maxResultsSize: Long,
    totalNumUpdatedBlockStatuses: Long,
    totalDiskBytesSpilled: Long,
    totalMemoryBytesSpilled: Long,
    maxPeaksExecutionMemory: Long,
    totalInputRecordsRead: Long,
    totalInputBytesRead: Long,
    totalOutputRecordsWritten: Long,
    totalOutputBytesWritten: Long,
    totalShuffleReadFetchWaitTime: Long,
    totalShuffleReadTotalBytesRead: Long,
    totalShuffleReadTotalBlocksFetched: Long,
    totalShuffleReadLocalBlocksFetched: Long,
    totalShuffleReadRemoteBlocksFetched: Long,
    totalShuffleWriteWriteTime: Long,
    totalShuffleWriteBytesWritten: Long,
    totalShuffleWriteRecordsWritten: Long)

  case class AggregatedRDDsPerformanceMetrics(
    numRDDs: Int,
    totalNumPartitions: Int,
    numCachedRDDs: Int,
    totalNumCachedPartitions: Int,
    totalCachedMemSize: Long,
    totalCachedDiskSize: Long,
    totalCachedExternalBlockStoreSize: Long)

  val performanceReportPosFileNameCSV = "-report.csv"
  val performanceReportPosFileNameJSON = "-report.json"

  var performanceReportConfProfile = "conf1"
  var performanceReportType = "print"
  var isSavePerformanceReport = false
  var performanceReportersFolder = ""

  var appId = "app-without-id"
  var appName = ""
  var applicationInitialTime = 0L
  var applicationFinalTime = 0L
  var applicationDuration = 0L

  var numJobs = 0
  var numFailedJobs = 0

  var numStages = 0
  var numFailedStages = 0
  val submissionTimes = scala.collection.mutable.ArrayBuffer.empty[Long]
  val completionTimes = scala.collection.mutable.ArrayBuffer.empty[Long]
  var totalStagesDuration = 0L

  var numTasks = 0
  var numSuccessfulTasks = 0
  var numFailedTasks = 0
  var numKilledTasks = 0
  var totalExecutionRunTime = 0L
  var totalExecutionCpuTime = 0L
  var totalExecutorDeserializeTime = 0L
  var totalExecutorDeserializeCpuTime = 0L
  var totalResultSerializationTime = 0L
  var totalJvmGCTime = 0L
  val resultsSize = scala.collection.mutable.ArrayBuffer.empty[Long]
  var totalNumUpdatedBlockStatuses = 0
  var totalDiskBytesSpilled = 0L
  var totalMemoryBytesSpilled = 0L
  val peaksExecutionMemory = scala.collection.mutable.ArrayBuffer.empty[Long]
  var totalInputRecordsRead = 0L
  var totalInputBytesRead = 0L
  var totalOutputRecordsWritten = 0L
  var totalOutputBytesWritten = 0L
  var totalShuffleReadFetchWaitTime = 0L
  var totalShuffleReadTotalBytesRead = 0L
  var totalShuffleReadTotalBlocksFetched = 0L
  var totalShuffleReadLocalBlocksFetched = 0L
  var totalShuffleReadRemoteBlocksFetched = 0L
  var totalShuffleWriteWriteTime = 0L
  var totalShuffleWriteBytesWritten = 0L
  var totalShuffleWriteRecordsWritten = 0L

  val visitedRDDs = scala.collection.mutable.Set.empty[Int]
  var numRDDs = 0
  var totalNumPartitions = 0
  var numCachedRDDs = 0
  var totalNumCachedPartitions = 0
  var totalCachedMemSize = 0L
  var totalCachedDiskSize = 0L
  var totalCachedExternalBlockStoreSize = 0L

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    appId = applicationStart.appId.getOrElse("app-without-id")
    appName = applicationStart.appName
    applicationInitialTime = applicationStart.time
 
    if (conf.contains("spark.performance.metrics.reports.configurationProfile")) {
      performanceReportConfProfile = conf.get("spark.performance.metrics.reports.configurationProfile")
    }
    if (conf.contains("spark.performance.metrics.reports.type")) {
      performanceReportType = conf.get("spark.performance.metrics.reports.type")
    }
    isSavePerformanceReport = conf.contains("spark.performance.metrics.reports.folder")
    if (isSavePerformanceReport)
      performanceReportersFolder = conf.get("spark.performance.metrics.reports.folder")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    applicationFinalTime = applicationEnd.time
    applicationDuration = applicationFinalTime - applicationInitialTime

    val tasksPerformanceMetrics = AggregatedTasksPerformanceMetrics(
      numTasks,
      numSuccessfulTasks,
      numFailedTasks,
      numKilledTasks,
      totalExecutionRunTime,
      totalExecutionCpuTime,
      totalExecutorDeserializeTime,
      totalExecutorDeserializeCpuTime,
      totalResultSerializationTime,
      totalJvmGCTime,
      if (!resultsSize.isEmpty) resultsSize.max else 0L,
      totalNumUpdatedBlockStatuses,
      totalDiskBytesSpilled,
      totalMemoryBytesSpilled,
      if (!peaksExecutionMemory.isEmpty) peaksExecutionMemory.max else 0L,
      totalInputRecordsRead,
      totalInputBytesRead,
      totalOutputRecordsWritten,
      totalOutputBytesWritten,
      totalShuffleReadFetchWaitTime,
      totalShuffleReadTotalBytesRead,
      totalShuffleReadTotalBlocksFetched,
      totalShuffleReadLocalBlocksFetched,
      totalShuffleReadRemoteBlocksFetched,
      totalShuffleWriteWriteTime,
      totalShuffleWriteBytesWritten,
      totalShuffleWriteRecordsWritten)

    val rddsPerformanceMetrics = AggregatedRDDsPerformanceMetrics(
      numRDDs,
      totalNumPartitions,
      numCachedRDDs,
      totalNumCachedPartitions,
      totalCachedMemSize,
      totalCachedDiskSize,
      totalCachedExternalBlockStoreSize)

    val stagesPerformanceMetrics = AggragatedStagesPerformanceMetrics(
      numStages,
      numFailedStages,
      (if (!completionTimes.isEmpty) completionTimes.max else 0L) - (if (!submissionTimes.isEmpty) submissionTimes.min else 0L),
      totalStagesDuration,
      tasksPerformanceMetrics,
      rddsPerformanceMetrics)

    val applicationPerformanceMetrics = ApplicationPerformanceMetrics(
      appId,
      appName,
      performanceReportConfProfile,
      applicationInitialTime,
      applicationFinalTime,
      applicationDuration,
      numJobs,
      numFailedJobs,
      stagesPerformanceMetrics)

    if (!performanceReportType.equals("print") && isSavePerformanceReport) {
      performanceReportType match {
        case "csv"  => savePerformaMetricsReportCSV(applicationPerformanceMetrics)
        case "json" => savePerformaMetricsReportJSON(applicationPerformanceMetrics)
        case "json_csv" => {
          savePerformaMetricsReportCSV(applicationPerformanceMetrics)
          savePerformaMetricsReportJSON(applicationPerformanceMetrics)
        }
        case "all" => {
          printPerformaMetrics(applicationPerformanceMetrics)
          savePerformaMetricsReportCSV(applicationPerformanceMetrics)
          savePerformaMetricsReportJSON(applicationPerformanceMetrics)
        }
        case _ => printPerformaMetrics(applicationPerformanceMetrics)
      }
    } else {
      printPerformaMetrics(applicationPerformanceMetrics)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    numJobs += 1
    if (jobEnd.jobResult.getClass.getName.contains("JobFailed"))
      numFailedJobs += 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    if (taskEnd.taskInfo.successful)
      numSuccessfulTasks += 1

    if (taskEnd.taskInfo.failed)
      numFailedTasks += 1

    if (taskEnd.taskInfo.killed)
      numKilledTasks += 1
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val stageInfo = stageCompleted.stageInfo
    val taskMetrics = stageInfo.taskMetrics
    val rddInfos = stageInfo.rddInfos
    numStages += 1
    if (stageInfo.failureReason.isDefined)
      numFailedStages += 1
    submissionTimes += stageInfo.submissionTime.getOrElse(0L)
    completionTimes += stageInfo.completionTime.getOrElse(0L)
    totalStagesDuration += stageInfo.completionTime.getOrElse(0L) - stageInfo.submissionTime.getOrElse(0L)
    numTasks += stageInfo.numTasks
    updateMetricsForTask(taskMetrics)
    updateMetricsForRDDs(rddInfos)
  }

  private def updateMetricsForTask(taskMetrics: TaskMetrics): Unit = {
    totalExecutionRunTime += taskMetrics.executorRunTime
    totalExecutionCpuTime += taskMetrics.executorCpuTime / 1000000
    totalExecutorDeserializeTime += taskMetrics.executorDeserializeTime
    totalExecutorDeserializeCpuTime += taskMetrics.executorDeserializeCpuTime / 1000000
    totalResultSerializationTime += taskMetrics.resultSerializationTime
    totalJvmGCTime += taskMetrics.jvmGCTime
    resultsSize += taskMetrics.resultSize
    totalNumUpdatedBlockStatuses += taskMetrics.updatedBlockStatuses.length
    totalDiskBytesSpilled = taskMetrics.diskBytesSpilled
    totalMemoryBytesSpilled = taskMetrics.memoryBytesSpilled
    peaksExecutionMemory += taskMetrics.peakExecutionMemory
    totalInputRecordsRead += taskMetrics.inputMetrics.recordsRead
    totalInputBytesRead += taskMetrics.inputMetrics.bytesRead
    totalOutputRecordsWritten += taskMetrics.outputMetrics.recordsWritten
    totalOutputBytesWritten += taskMetrics.outputMetrics.bytesWritten
    totalShuffleReadFetchWaitTime += taskMetrics.shuffleReadMetrics.fetchWaitTime
    totalShuffleReadTotalBytesRead = taskMetrics.shuffleReadMetrics.totalBytesRead
    totalShuffleReadTotalBlocksFetched = taskMetrics.shuffleReadMetrics.totalBlocksFetched
    totalShuffleReadLocalBlocksFetched = taskMetrics.shuffleReadMetrics.localBlocksFetched
    totalShuffleReadRemoteBlocksFetched = taskMetrics.shuffleReadMetrics.remoteBlocksFetched
    totalShuffleWriteWriteTime += taskMetrics.shuffleWriteMetrics.writeTime / 1000000
    totalShuffleWriteBytesWritten += taskMetrics.shuffleWriteMetrics.bytesWritten
    totalShuffleWriteRecordsWritten += taskMetrics.shuffleWriteMetrics.recordsWritten
  }

  private def updateMetricsForRDDs(rddInfos: Seq[RDDInfo]): Unit = {
    for (rdd <- rddInfos) {
      if (!visitedRDDs.contains(rdd.id)) {
        visitedRDDs += rdd.id
        numRDDs += 1
        totalNumPartitions += rdd.numPartitions
        if (rdd.isCached) numCachedRDDs += 1
        totalNumCachedPartitions += rdd.numCachedPartitions
        totalCachedMemSize += rdd.memSize
        totalCachedDiskSize += rdd.diskSize
        totalCachedExternalBlockStoreSize += rdd.externalBlockStoreSize
      }
    }
  }

  def printPerformaMetrics(metrics: ApplicationPerformanceMetrics): Unit = {
    val stagesMetrics = metrics.stagesPerformanceMetrics
    val tasksMetrics = metrics.stagesPerformanceMetrics.tasksPerformanceMetrics
    val rddsMetrics = metrics.stagesPerformanceMetrics.rddsPerformanceMetrics

    println("Metrics of application: " + metrics.appName + " (" + metrics.appId + ")")
    println("Application Duration: " + formatDuration(metrics.applicationDuration))
    println("Configuration Profile: " + metrics.configurationProfile)
    println("Number of Jobs: " + metrics.numJobs)
    println("Number of Failed Jobs: " + metrics.numFailedJobs)
    println("Number of Stages: " + stagesMetrics.numStages)
    println("Number of Failed Stages: " + stagesMetrics.numFailedStages)
    println("Total Stages Elapsed Time: " + formatDuration(stagesMetrics.totalStagesElapsedTime))
    println("Total Stages Duration: " + formatDuration(stagesMetrics.totalStagesDuration))
    println("Number of Tasks: " + tasksMetrics.numTasks)
    println("Number of Successful Tasks: " + tasksMetrics.numSuccessfulTasks)
    println("Number of Failed Tasks: " + tasksMetrics.numFailedTasks)
    println("Number of Killed Tasks: " + tasksMetrics.numKilledTasks)
    println("Total Execution RunTime: " + formatDuration(tasksMetrics.totalExecutionRunTime))
    println("Total Execution CpuTime: " + formatDuration(tasksMetrics.totalExecutionCpuTime))
    println("Total Executor Deserialize Time: " + formatDuration(tasksMetrics.totalExecutorDeserializeTime))
    println("Total Executor Deserialize CpuTime: " + formatDuration(tasksMetrics.totalExecutorDeserializeCpuTime))
    println("Total Result Serialization Time: " + formatDuration(tasksMetrics.totalResultSerializationTime))
    println("Total jvmGCTime: " + formatDuration(tasksMetrics.totalJvmGCTime))
    println("Max Result Size: " + formatBytes(tasksMetrics.maxResultsSize))
    println("Total Num Updated Block Statuses: " + tasksMetrics.totalNumUpdatedBlockStatuses)
    println("Total Disk Bytes Spilled: " + formatBytes(tasksMetrics.totalDiskBytesSpilled))
    println("Total Memory Bytes Spilled: " + formatBytes(tasksMetrics.totalMemoryBytesSpilled))
    println("Max Peaks Execution Memory: " + tasksMetrics.maxPeaksExecutionMemory)
    println("Total Input Records Read: " + tasksMetrics.totalInputRecordsRead)
    println("Total Input Bytes Read: " + formatBytes(tasksMetrics.totalInputBytesRead))
    println("Total Output Records Written: " + tasksMetrics.totalOutputRecordsWritten)
    println("Total Output Bytes Written: " + formatBytes(tasksMetrics.totalOutputBytesWritten))
    println("Total Shuffle Read Fetch WaitTime: " + formatDuration(tasksMetrics.totalShuffleReadFetchWaitTime))
    println("Total Shuffle Read Total Bytes Read: " + formatBytes(tasksMetrics.totalShuffleReadTotalBytesRead))
    println("Total Shuffle Read Total Blocks Fetched: " + tasksMetrics.totalShuffleReadTotalBlocksFetched)
    println("Total Shuffle Read Local Blocks Fetched: " + tasksMetrics.totalShuffleReadLocalBlocksFetched)
    println("Total Shuffle Read Remote Blocks Fetched: " + tasksMetrics.totalShuffleReadRemoteBlocksFetched)
    println("Total Shuffle Write Write Time: " + formatDuration(tasksMetrics.totalShuffleWriteWriteTime))
    println("Total Shuffle Write Bytes Written: " + formatBytes(tasksMetrics.totalShuffleWriteBytesWritten))
    println("Total Shuffle Write Records Written: " + tasksMetrics.totalShuffleWriteRecordsWritten)
    println("Number of RDDs: " + rddsMetrics.numRDDs)
    println("Total Number of Partitions: " + rddsMetrics.totalNumPartitions)
    println("Number of Cached RDDs: " + rddsMetrics.numCachedRDDs)
    println("Total Number of Cached Partitions: " + rddsMetrics.totalNumCachedPartitions)
    println("Total Cached in Memory: " + formatBytes(rddsMetrics.totalCachedMemSize))
    println("Total Cached in Disk: " + formatBytes(rddsMetrics.totalCachedDiskSize))
    println("Total Cached in External Block Store: " + formatBytes(rddsMetrics.totalCachedExternalBlockStoreSize))
  }

  def savePerformaMetricsReportCSV(metrics: ApplicationPerformanceMetrics): Unit = {
    val stagesMetrics = metrics.stagesPerformanceMetrics
    val tasksMetrics = metrics.stagesPerformanceMetrics.tasksPerformanceMetrics
    val rddsMetrics = metrics.stagesPerformanceMetrics.rddsPerformanceMetrics

    val folder = new File(performanceReportersFolder)
    val folderCSV = new File(performanceReportersFolder + File.separator + "csv")
    if (folder.exists() && folder.isDirectory() && folderCSV.exists() && folderCSV.isDirectory()) {
      val reportFileName = metrics.appName + "-" + metrics.appId + performanceReportPosFileNameCSV
      val reportFile = new File(performanceReportersFolder + File.separator + "csv" + File.separator + reportFileName)
      val writer = CSVWriter.open(reportFile)

      val headersRow = List(
        "appId",
        "appName",
        "configurationProfile",
        "initialTime",
        "finalTime",
        "applicationDuration",
        "numJobs",
        "numFailedJobs",

        "numStages",
        "numFailedStages",
        "totalStagesElapsedTime",
        "totalStagesDuration",

        "numTasks",
        "numSuccessfulTasks",
        "numFailedTasks",
        "numKilledTasks",
        "totalExecutionRunTime",
        "totalExecutionCpuTime",
        "totalExecutorDeserializeTime",
        "totalExecutorDeserializeCpuTime",
        "totalResultSerializationTime",
        "totalJvmGCTime",
        "maxResultsSize",
        "totalNumUpdatedBlockStatuses",
        "totalDiskBytesSpilled",
        "totalMemoryBytesSpilled",
        "maxPeaksExecutionMemory",
        "totalInputRecordsRead",
        "totalInputBytesRead",
        "totalOutputRecordsWritten",
        "totalOutputBytesWritten",
        "totalShuffleReadFetchWaitTime",
        "totalShuffleReadTotalBytesRead",
        "totalShuffleReadTotalBlocksFetched",
        "totalShuffleReadLocalBlocksFetched",
        "totalShuffleReadRemoteBlocksFetched",
        "totalShuffleWriteWriteTime",
        "totalShuffleWriteBytesWritten",
        "totalShuffleWriteRecordsWritten",

        "numRDDs",
        "totalNumPartitions",
        "numCachedRDDs",
        "totalNumCachedPartitions",
        "totalCachedMemSize",
        "totalCachedDiskSize",
        "totalCachedExternalBlockStoreSize")

      val valuesRow = List(
        metrics.appId,
        metrics.appName,
        metrics.configurationProfile,
        metrics.initialTime,
        metrics.finalTime,
        metrics.applicationDuration,
        metrics.numJobs,
        metrics.numFailedJobs,

        stagesMetrics.numStages,
        stagesMetrics.numFailedStages,
        stagesMetrics.totalStagesElapsedTime,
        stagesMetrics.totalStagesDuration,

        tasksMetrics.numTasks,
        tasksMetrics.numSuccessfulTasks,
        tasksMetrics.numFailedTasks,
        tasksMetrics.numKilledTasks,
        tasksMetrics.totalExecutionRunTime,
        tasksMetrics.totalExecutionCpuTime,
        tasksMetrics.totalExecutorDeserializeTime,
        tasksMetrics.totalExecutorDeserializeCpuTime,
        tasksMetrics.totalResultSerializationTime,
        tasksMetrics.totalJvmGCTime,
        tasksMetrics.maxResultsSize,
        tasksMetrics.totalNumUpdatedBlockStatuses,
        tasksMetrics.totalDiskBytesSpilled,
        tasksMetrics.totalMemoryBytesSpilled,
        tasksMetrics.maxPeaksExecutionMemory,
        tasksMetrics.totalInputRecordsRead,
        tasksMetrics.totalInputBytesRead,
        tasksMetrics.totalOutputRecordsWritten,
        tasksMetrics.totalOutputBytesWritten,
        tasksMetrics.totalShuffleReadFetchWaitTime,
        tasksMetrics.totalShuffleReadTotalBytesRead,
        tasksMetrics.totalShuffleReadTotalBlocksFetched,
        tasksMetrics.totalShuffleReadLocalBlocksFetched,
        tasksMetrics.totalShuffleReadRemoteBlocksFetched,
        tasksMetrics.totalShuffleWriteWriteTime,
        tasksMetrics.totalShuffleWriteBytesWritten,
        tasksMetrics.totalShuffleWriteRecordsWritten,

        rddsMetrics.numRDDs,
        rddsMetrics.totalNumPartitions,
        rddsMetrics.numCachedRDDs,
        rddsMetrics.totalNumCachedPartitions,
        rddsMetrics.totalCachedMemSize,
        rddsMetrics.totalCachedDiskSize,
        rddsMetrics.totalCachedExternalBlockStoreSize)

      //writer.writeRow(headersRow)
      writer.writeRow(valuesRow)
      writer.close()
      println("CSV Performance Report Saved: " + reportFile.getAbsolutePath)
    } else {
      println("CSV Performance Report Not Saved")
      println("Invalid Performance Reports Folder: " + performanceReportersFolder)
    }
  }

  def savePerformaMetricsReportJSON(metrics: ApplicationPerformanceMetrics): Unit = {
    val folder = new File(performanceReportersFolder)
    val folderJSON = new File(performanceReportersFolder + File.separator + "json")
    if (folder.exists() && folder.isDirectory() && folderJSON.exists() && folderJSON.isDirectory()) {
      val reportFileName = metrics.appName + "-" + metrics.appId + performanceReportPosFileNameJSON
      val reportFile = new File(performanceReportersFolder + File.separator + "json" + File.separator + reportFileName)
      val writer = new PrintWriter(reportFile)
      val gson = (new GsonBuilder()).create
      writer.write(gson.toJson(metrics))
      writer.close()
      println("JSON Performance Report Saved: " + reportFile.getAbsolutePath)
    } else {
      println("JSON Performance Report Not Saved")
      println("Invalid Performance Reports Folder: " + performanceReportersFolder)
    }
  }
}