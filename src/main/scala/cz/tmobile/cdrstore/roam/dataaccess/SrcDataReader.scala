package cz.tmobile.cdrstore.roam.dataaccess

import org.apache.spark.sql.DataFrame

trait SrcDataReader {
  def readData(eventType: String) : DataFrame
}
