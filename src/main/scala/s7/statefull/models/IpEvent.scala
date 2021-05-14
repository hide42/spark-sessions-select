package s7.statefull.models

import java.sql.Timestamp
import java.time.Instant

case class IpEvent(
                    ip: Int,
                    url: String,
                    timestamp: Timestamp = Timestamp.from(Instant.now())
                    )
