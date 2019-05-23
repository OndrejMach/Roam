package cz.tmobile.cdrstore.roam.processors

import cz.tmobile.cdrstore.roam.config.Config
import cz.tmobile.cdrstore.roam.dataaccess.{AnonColumnsOut, AnonEventDatesColumns, AnonEventsColumns, CallEventTypes, EventDatesReader, EventReader, EventWriter,SrcDataReader}
import org.apache.spark.sql.{DataFrame, SparkSession}


class AnonProcessor(srcDataReader: SrcDataReader, eventReader: EventReader, eventDatesReader: EventDatesReader, eventWriter: EventWriter, config: Config)(implicit sparkSession: SparkSession) extends Processor {
  def mergeStageandSRCData(stage: DataFrame, src: DataFrame) : DataFrame = {
    if (src.rdd.isEmpty()) {
      stage
    } else {
      stage.union(src)
    }
  }

  def joinWithEventDates(events: DataFrame, eventDates: DataFrame, eventType: String) :DataFrame = {
    events
      .join(eventDates.filter(eventDates(AnonEventDatesColumns.event_type.toString) === eventType),
        eventDates.col(AnonEventDatesColumns.event_date.toString) === events.col(AnonEventsColumns.call_event_date.toString))
      .drop(eventDates.col(AnonEventDatesColumns.event_date.toString))
  }

  def calculateTacFrequencies(events: DataFrame) : DataFrame = {
    events
      .select(AnonEventsColumns.imei_hash.toString, AnonEventsColumns.tac_code.toString, AnonEventsColumns.call_event_date.toString)
      .distinct()
      .select(AnonEventsColumns.tac_code.toString, AnonEventsColumns.call_event_date.toString)
      .groupBy(AnonEventsColumns.tac_code.toString, AnonEventsColumns.call_event_date.toString)
      .count()
      .withColumnRenamed("count", AnonColumnsOut.tac_frequency.toString)
  }

  def getWithFrequencies(events: DataFrame, frequencies: DataFrame) :DataFrame = {
    events
      .join(frequencies, (frequencies(AnonEventsColumns.tac_code.toString) === events(AnonEventsColumns.tac_code.toString))
        && (frequencies(AnonEventsColumns.call_event_date.toString) === events(AnonEventsColumns.call_event_date.toString)), "left")
      .drop(frequencies(AnonEventsColumns.call_event_date.toString))
      .drop(frequencies(AnonEventsColumns.tac_code.toString))
  }


  def run(): Unit = {
    logger.info(s"Reading event dates")
    val eventDates = eventDatesReader.readData(CallEventTypes.values.toSeq.map(_.toString))

    if (eventDates.rdd.isEmpty()) {

      logger.warn("Event dates empty, no data to process")

    } else {

      CallEventTypes.values.toSeq.foreach { eventType =>
        val event = eventType.toString

        logger.info(s"Reading events for $event")
        val eventsStage = eventReader.readStageData(event)

        if (eventsStage.rdd.isEmpty()) {
          logger.warn("No stage data to process")
        } else {

          logger.info(s"reading src data for $event")
          val srcData = srcDataReader.readData(event)

          logger.info("Merging SRC and STAGE data")
          val inputData = mergeStageandSRCData(eventsStage, srcData)

          logger.info("Joining input data with call event dates table")
          val eventsForDates = joinWithEventDates(inputData, eventDates, event)

          logger.info("Calculating TAC frequencies")
          val tacFrequencies = calculateTacFrequencies(eventsForDates)

          logger.info("Merging everything togethes")
          val withFrequencies = getWithFrequencies(eventsForDates, tacFrequencies)

          logger.info("Writing output")
          val partitioningColumns = Seq(AnonEventDatesColumns.event_type.toString, AnonEventsColumns.call_event_date.toString)
          eventWriter.writeData(withFrequencies,
            partitioningColumns,
            config.parameters.anonTmpDir)
        }
      }
    }
  }
}