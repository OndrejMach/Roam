package cz.tmobile.cdrstore.roam.common

import org.slf4j.LoggerFactory

trait Logger {
  lazy val logger = LoggerFactory.getLogger(getClass)
}
