package cz.tmobile.cdrstore.roam.config
import com.typesafe.config.ConfigFactory
import scala.util.Properties

case class AnonConfig(anonTadig: String, anonTmpDir: String, stageDir: String, callEventsDateFile: String)


class Config (fileNameOption: Option[String] = None){
  private val config = fileNameOption.fold(
    ifEmpty = ConfigFactory.load() )(
    file => ConfigFactory.load(file) )

  lazy val parameters =
    new AnonConfig(
      anonTadig = envOrElseConfig("configuration.ANON_TADIG.value"),
      anonTmpDir = envOrElseConfig("configuration.ANON_TMP_DIR.value"),
      stageDir = envOrElseConfig("configuration.STAGE_DIR.value"),
      callEventsDateFile = envOrElseConfig("configuration.CALL_EVENT_DATES_FILE.value")
    )



  private def envOrElseConfig(name: String): String = {
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }


}
