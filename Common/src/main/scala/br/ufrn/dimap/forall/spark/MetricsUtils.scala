package br.ufrn.dimap.forall.spark

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

object MetricsUtils {

  // SimpleDateFormat is not thread-safe. Don't expose it to avoid improper use.
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat =
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.US)
  }

  def formatDate(date: Date): String = dateFormat.get.format(date)

  def formatDate(timestamp: Long): String = dateFormat.get.format(new Date(timestamp))

  def formatDuration(milliseconds: Long): String = {
    if (milliseconds < 100) {
      return "%d ms".format(milliseconds)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 1) {
      return "%.1f s".format(seconds)
    }
    if (seconds < 60) {
      return "%.0f s".format(seconds)
    }
    val minutes = seconds / 60
    if (minutes < 10) {
      return "%.1f min".format(minutes)
    } else if (minutes < 60) {
      return "%.0f min".format(minutes)
    }
    val hours = minutes / 60
    "%.1f h".format(hours)
  }

  def formatBytes(bytes: Long): String = {
    val trillion = 1024L * 1024L * 1024L * 1024L
    val billion = 1024L * 1024L * 1024L
    val million = 1024L * 1024L
    val thousand = 1024L

    val (value, unit): (Double, String) = {
      if (bytes >= 2 * trillion) {
        (bytes / trillion, " TB")
      } else if (bytes >= 2 * billion) {
        (bytes / billion, " GB")
      } else if (bytes >= 2 * million) {
        (bytes / million, " MB")
      } else if (bytes >= 2 * thousand) {
        (bytes / thousand, " KB")
      } else {
        (bytes, " Bytes")
      }
    }
    if (unit == " Bytes") {
      "%d%s".format(value.toInt, unit)
    } else {
      "%.1f%s".format(value, unit)
    }
  }

  /** Generate a verbose human-readable string representing a duration such as "5 second 35 ms" */
  def formatDurationVerbose(ms: Long): String = {
    try {
      val second = 1000L
      val minute = 60 * second
      val hour = 60 * minute
      val day = 24 * hour
      val week = 7 * day
      val year = 365 * day

      def toString(num: Long, unit: String): String = {
        if (num == 0) {
          ""
        } else if (num == 1) {
          s"$num $unit"
        } else {
          s"$num ${unit}s"
        }
      }

      val millisecondsString = if (ms >= second && ms % second == 0) "" else s"${ms % second} ms"
      val secondString = toString((ms % minute) / second, "second")
      val minuteString = toString((ms % hour) / minute, "minute")
      val hourString = toString((ms % day) / hour, "hour")
      val dayString = toString((ms % week) / day, "day")
      val weekString = toString((ms % year) / week, "week")
      val yearString = toString(ms / year, "year")

      Seq(
        second -> millisecondsString,
        minute -> s"$secondString $millisecondsString",
        hour -> s"$minuteString $secondString",
        day -> s"$hourString $minuteString $secondString",
        week -> s"$dayString $hourString $minuteString",
        year -> s"$weekString $dayString $hourString").foreach {
          case (durationLimit, durationString) =>
            if (ms < durationLimit) {
              // if time is less than the limit (upto year)
              return durationString
            }
        }
      // if time is more than a year
      return s"$yearString $weekString $dayString"
    } catch {
      case e: Exception =>
        // if there is some error, return blank string
        return ""
    }
  }

  /** Generate a human-readable string representing a number (e.g. 100 K) */
  def formatNumber(records: Double): String = {
    val trillion = 1e12
    val billion = 1e9
    val million = 1e6
    val thousand = 1e3

    val (value, unit) = {
      if (records >= 2 * trillion) {
        (records / trillion, " T")
      } else if (records >= 2 * billion) {
        (records / billion, " B")
      } else if (records >= 2 * million) {
        (records / million, " M")
      } else if (records >= 2 * thousand) {
        (records / thousand, " K")
      } else {
        (records, "")
      }
    }
    if (unit.isEmpty) {
      "%d".formatLocal(Locale.US, value.toInt)
    } else {
      "%.1f%s".formatLocal(Locale.US, value, unit)
    }
  }

}