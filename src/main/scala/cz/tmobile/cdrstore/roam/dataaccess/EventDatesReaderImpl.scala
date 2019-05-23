package cz.tmobile.cdrstore.roam.dataaccess

import cz.tmobile.cdrstore.roam.common.{HDFSUtils, Logger}
import cz.tmobile.cdrstore.roam.config.Config
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class EventDatesReaderImpl(config: Config)(implicit sparkSession: SparkSession) extends EventDatesReader with Logger{
  private def readParquet(path: String, event: String): DataFrame = {
    val schema = new StructType()
      .add(AnonEventDatesColumns.event_date.toString,StringType,false)

    logger.info(s"Reading file ${path} for event type ${event}")
    val df = sparkSession.read.schema(schema).csv(path)
    df.withColumn(AnonEventDatesColumns.event_type.toString, lit(event))
  }

  override def readData(events: Seq[String]): DataFrame = {
    logger.info(s"Trying to read folders: ${events.mkString(",")}")
    val paths = for {event <- events if (HDFSUtils.dirExist(s"${config.parameters.callEventsDateFile}.${event}"))}
      yield {
        readParquet(s"${config.parameters.callEventsDateFile}.${event}", event)
      }
    paths.reduce(_.union(_))
  }
}
