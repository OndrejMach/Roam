package cz.tmobile.cdrstore.roam.dataaccess

import cz.tmobile.cdrstore.roam.common.{HDFSUtils, Logger}
import cz.tmobile.cdrstore.roam.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class SrcDataReaderImpl(config: Config)(implicit sparkSession: SparkSession) extends SrcDataReader with Logger {
  override def readData(eventType: String): DataFrame = {
    val path = s"${config.parameters.anonTadig}/call_event_detail=${eventType}/"
    if (HDFSUtils.dirExist(path)) {
      sparkSession.read.parquet("path")
    }
    else {
      sparkSession.emptyDataFrame
    }
  }
}
