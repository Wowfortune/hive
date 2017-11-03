package org.apache.hive.service.cli;

import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.util.StringUtils;

import java.io.StringWriter;
import java.util.List;

public class JobProgressUpdate {
  public final double progressedPercentage;
  public final String footerSummary;
  public final long startTimeMillis;
  private final List<String> headers;
  private final List<List<String>> rows;
  public final String status;


  JobProgressUpdate(ProgressMonitor monitor) {
    this(monitor.headers(), monitor.rows(), monitor.footerSummary(), monitor.progressedPercentage(),
        monitor.startTime(), monitor.executionStatus());
  }

  private JobProgressUpdate(List<String> headers, List<List<String>> rows, String footerSummary,
      double progressedPercentage, long startTimeMillis, String status) {
    this.progressedPercentage = progressedPercentage;
    this.footerSummary = footerSummary;
    this.startTimeMillis = startTimeMillis;
    this.headers = headers;
    this.rows = rows;
    this.status = status;
  }

  public List<String> headers() {
    return headers;
  }

  public List<List<String>> rows() {
    return rows;
  }

  @Override
  public String toString() {
    return "JobProgressUpdate{" +
        "progressedPercentage=" + progressedPercentage +
        ", footerSummary='" + footerSummary + '\'' +
        ", startTimeMillis=" + startTimeMillis +
        ", headers='" + forCollection(headers) + '\'' +
        ", rows='" + forNestedCollection(rows) + '\'' +
        ", status='" + status + '\'' +
        '}';
  }

  private String forNestedCollection(List<List<String>> collection) {
    if (collection == null) {
      return "";
    }
    StringWriter writer = new StringWriter();
    for (List list : collection) {
      writer.append(forCollection(list)).append(" / ");
    }
    return writer.toString();
  }

  private String forCollection(List collection) {
    return collection == null ? "" : StringUtils.join(",", collection);
  }
}
