package cz.tmobile.cdrstore.roam.dataaccess

import cz.tmobile.cdrstore.roam.common.{HDFSUtils, Logger}
import cz.tmobile.cdrstore.roam.config.Config
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class EventReaderImpl(config: Config)(implicit sparkSession: SparkSession) extends EventReader with Logger {
  override def readSrcData(EventType: String, date: String): DataFrame = sparkSession.emptyDataFrame

  override def readStageData(eventType: String): DataFrame = {
    val path = s"${config.parameters.stageDir}.${eventType}"
    if (HDFSUtils.dirExist(path)){
      logger.info(s"Reading stage data ${path}")
      sparkSession.read.parquet(path).withColumn("eventType", lit(eventType))
    } else {
      logger.warn(s"Path ${path} does not exist, data empty")
      sparkSession.emptyDataFrame
    }
  }
}
