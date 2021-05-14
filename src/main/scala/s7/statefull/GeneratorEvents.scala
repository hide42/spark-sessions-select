package s7.statefull

import java.sql.Timestamp
import java.time.Instant

import s7.statefull.models.IpEvent

import scala.util.Random

class GeneratorEvents {
  var incTime = 0
  var r = new Random()

  def generateEvent(id: Int): IpEvent = {
    incTime += 1
    models.IpEvent(id, "s7.ru/" + r.nextInt(20), Timestamp.from(Instant.now().plusSeconds(incTime)))
  }

  def generate(): Seq[IpEvent] = {
    println("Generating 5 events.")
    Seq(
      generateEvent(r.nextInt(3)),
      generateEvent(r.nextInt(3)),
      generateEvent(r.nextInt(3)),
      generateEvent(r.nextInt(3)),
      generateEvent(r.nextInt(3))
    )
  }
}