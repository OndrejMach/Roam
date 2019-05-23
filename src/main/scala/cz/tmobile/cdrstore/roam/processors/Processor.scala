package cz.tmobile.cdrstore.roam.processors

import cz.tmobile.cdrstore.roam.common.Logger

trait Processor extends Logger {
  def run() : Unit
}
