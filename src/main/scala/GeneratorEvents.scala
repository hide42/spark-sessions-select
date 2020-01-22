import java.sql.Timestamp
import java.time.Instant

import models.IpEvent

import scala.util.Random

class GeneratorEvents {
  var incTime = 0
  var r = new Random()
  def generateEvent(id:Int):IpEvent={
    incTime+=1
    models.IpEvent(id,"google.com/"+r.nextInt(20),Timestamp.from(Instant.now().plusSeconds(incTime)))
  }
  def generate():Seq[IpEvent]={
    Seq(
      generateEvent(r.nextInt(3)),
      generateEvent(r.nextInt(3)),
      generateEvent(r.nextInt(3)),
      generateEvent(r.nextInt(3)),
      generateEvent(r.nextInt(3))
    )
  }
}
