package models

import java.sql.Timestamp

case class IpStatistics(
                         ip: Int,
                         visits: Seq[IpEvent],
                         totalVisits: Int,
                         firstVisit: Timestamp,
                         lastTime: Timestamp,
                         totalTimeMS: Double
                         )
