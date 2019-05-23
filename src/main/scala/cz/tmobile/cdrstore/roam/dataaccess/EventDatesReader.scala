package cz.tmobile.cdrstore.roam.dataaccess

import org.apache.spark.sql.DataFrame

trait EventDatesReader {
  def readData(events: Seq[String]) : DataFrame
}
