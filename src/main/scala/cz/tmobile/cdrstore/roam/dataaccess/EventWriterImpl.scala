package cz.tmobile.cdrstore.roam.dataaccess

import cz.tmobile.cdrstore.roam.common.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

class EventWriterImpl extends EventWriter with Logger{
  override def writeData(input: DataFrame, partitionedBy: Seq[String], outputFile: String) = {
    input.write.mode(SaveMode.Overwrite).partitionBy(partitionedBy :_*).parquet(outputFile)
  }
}
