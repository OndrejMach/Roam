package cz.tmobile.cdrstore.roam
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat
import java.util.Date



object AnonLegacy {
  val sparkConf = new SparkConf()
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel(sys.env("SPARK_LOG_LEVEL"))
  val sqlContext = new SQLContext(sc)

  val hadoopfs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
  def dirExist(path: String): Boolean = {
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
  }

  def info(str: String) {
    val datum = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    println(s"$datum -- $str")
  }
/*
BODY ke zvlazeni:
 1) unifikovat nejak ten parquet, nejspis se pouziva stara knihovna na zapis - String vs. binary
 2) Call_Event_Details v 1 tabulce partitioning podle toho typu - napr. 'eventType=gprsCall' - usetrilo by se cteni parquetu
 3) ANON_TMP_DIR + "/call_event_detail=" + CALL_EVENT_DETAIL + "/call_event_date=" + CALL_EVENT_DATE neni treba pokud se pri zapisu (.write.parquet) da partition by, spark udela partitioning sam

 */



  def main(args: Array[String]) {

    // Variables from EVL job
    val ANON_TADIG = sys.env("ANON_TADIG")
    val ANON_TMP_DIR = sys.env("ANON_TMP_DIR")
    val STAGE_DIR  = sys.env("STAGE_DIR")
    val CALL_EVENT_DATES_FILE = sys.env("CALL_EVENT_DATES_FILE")

    val CALL_EVENT_DETAILS = List("gprsCall","mobileOriginatedCall","mobileTerminatedCall","supplServiceEvent")
    val ANON_COLUMNS = Seq("id","call_event_date","version","filename","sender","recipient","location_area","cause_for_term","file_sequence_number","rap_file_sequence_number","tap_currency","tap_decimal_places","rec_entity_code","charged_item","charge_type","call_type_level1","call_type_level2","call_type_level3","camel_flag","access_point_name_ni","tele_service_code","tac_code","local_timestamp","utc_time_offset","imsi_hash","imei_hash","device_type","manufacturer","msisdn_hash","msisdn_country_code","pdp_address_hash","cell_id_hash","charging_id","call_reference","event_reference","called_number_network_code","dialled_digits_network_code","called_party_gnf_iso2","third_party_number_network_code","camel_destination_number_network_code","sms_destination_number_network_code","calling_number_network_code","sms_originator_network_code","non_charged_party_number_network_code","requested_number_network_code","clir_indicator","total_call_event_duration","data_volume_incoming","data_volume_outgoing","charge","chargeable_units","charged_units","total_charge","total_chargeable_units","total_charged_units","tax_value","m2m_flag","json_object","load_date",
      "called_number_hash","dialled_digits","third_party_number","camel_destination_number","sms_destination_number","calling_number_hash","sms_originator","msisdn_trunc_hash","called_number_trunc_hash","calling_number_trunc_hash")

    for ( callEventDetail <- CALL_EVENT_DETAILS) {

      info(callEventDetail)

      // Get list of call_event_dates
      val STAGE = STAGE_DIR + "." + callEventDetail
      sqlContext.setConf("spark.sql.parquet.binaryAsString","true")
      val CALL_EVENT_DATES = sqlContext.read.csv(CALL_EVENT_DATES_FILE + "." + callEventDetail)
      sqlContext.setConf("spark.sql.parquet.binaryAsString","false")
      sqlContext.setConf("spark.sql.shuffle.partitions","10")

      for ( callEventRow <- CALL_EVENT_DATES.collect) {
        callEventRow.toSeq.foreach { CALL_EVENT_DATE =>

          info(CALL_EVENT_DATE.toString)

          val ANON_SRC = ANON_TADIG + "/call_event_detail=" + callEventDetail + "/call_event_date=" + CALL_EVENT_DATE
          val ANON_TMP = ANON_TMP_DIR + "/call_event_detail=" + callEventDetail + "/call_event_date=" + CALL_EVENT_DATE

          val stage_df_for_tac = sqlContext.read.parquet(STAGE).
            select("imei_hash", "tac_code").
            where("call_event_date = '" + CALL_EVENT_DATE + "'")

          // Distinguish first run for given call_event_date
          if (dirExist(ANON_SRC)) {
            val anon_df_for_tac = sqlContext.read.parquet(ANON_SRC).
              select("imei_hash", "tac_code").
              where("tac_code is not null")
            val tac_freq = anon_df_for_tac.union(stage_df_for_tac).
              distinct.
              select("tac_code").
              groupBy("tac_code").
              count.
              withColumnRenamed("count","tac_frequency").
              withColumnRenamed("tac_code","tac_code1")
            val stage_df = sqlContext.read.parquet(STAGE).
              select(ANON_COLUMNS.head, ANON_COLUMNS.tail: _*).
              where("call_event_date = '" + CALL_EVENT_DATE + "'")
            val joined = stage_df.join(tac_freq,
              stage_df("tac_code") === tac_freq("tac_code1"),
              "left").
              drop("tac_code1")

            joined.write.mode("overwrite").parquet(ANON_TMP)

            // Case of first run of given call_event_date
          } else {

            val tac_freq = stage_df_for_tac.
              distinct.
              select("tac_code").
              groupBy("tac_code").
              count.
              withColumnRenamed("count","tac_frequency").
              withColumnRenamed("tac_code","tac_code1")
            val stage_df = sqlContext.read.parquet(STAGE).
              select(ANON_COLUMNS.head, ANON_COLUMNS.tail: _*).
              where("call_event_date = '" + CALL_EVENT_DATE + "'")
            val joined = stage_df.join(tac_freq, stage_df("tac_code") === tac_freq("tac_code1"), "left").
              drop("tac_code1")

            joined.write.mode("overwrite").parquet(ANON_TMP)

          }

        } // end of CALL_EVENT_DATE
      }
    } // end of CALL_EVENT_DETAIL

  }

}
