package cz.tmobile.cdrstore.roam.common

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object HDFSUtils extends  Logger{
  def dirExist(path: String)(implicit sparkSession: SparkSession): Boolean = {
    val hadoopfs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val p = new Path(path)
    if (hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory && hadoopfs.listFiles(p,true).hasNext()){
      logger.info(s"Folder ${path} exists")
      true
    } else {
      logger.warn(s"Folder ${path} does not exist")
      false
    }
  }
}
