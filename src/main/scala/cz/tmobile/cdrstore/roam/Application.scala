package cz.tmobile.cdrstore.roam

import cz.tmobile.cdrstore.roam.common.Logger
import cz.tmobile.cdrstore.roam.config.Config
import cz.tmobile.cdrstore.roam.dataaccess.{EventDatesReader, EventDatesReaderImpl, EventReader, EventReaderImpl, EventWriter, EventWriterImpl, SrcDataReader, SrcDataReaderImpl}
import cz.tmobile.cdrstore.roam.processors.AnonProcessor
import org.apache.spark.sql.SparkSession

object Application extends App with Logger {

  //can do some more complex parameter parsing/validation

  if (args.length==0){
    logger.error("Please provide application name (only 'anon' supported now)")
    System.exit(1)
  }



  logger.info("Loading configuration file")
  val config = new Config()

  logger.info("Initialising Spark session")
  implicit val sparkSession = SparkSession
    .builder()
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .appName("Test")
    .master("local[*]")
    .getOrCreate()

  val eventDatesReader: EventDatesReader= new EventDatesReaderImpl(config)
  val eventReader : EventReader = new EventReaderImpl(config)
  val srcDataReader : SrcDataReader = new SrcDataReaderImpl(config)
  val outputWriter : EventWriter = new EventWriterImpl

  val application = args(0) match {
    case "anon" => new AnonProcessor(srcDataReader, eventReader, eventDatesReader, outputWriter, config)
    case _ => new AnonProcessor(srcDataReader, eventReader, eventDatesReader, outputWriter, config) //Just dummy, but ready for future enhancements - default processor or nothing
  }
  //application run - can be


  application.run()

}
