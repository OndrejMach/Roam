package cz.tmobile.cdrstore.roam.dataaccess

import org.apache.spark.sql.DataFrame

trait EventWriter {
  def writeData(input: DataFrame, partitionedBy: Seq[String], outputFile: String)
}
