package cz.tmobile.cdrstore.roam.dataaccess

import org.apache.spark.sql.DataFrame

trait EventReader {
  def readStageData(eventType: String) : DataFrame
  def readSrcData(EventType: String, date: String):DataFrame
}
