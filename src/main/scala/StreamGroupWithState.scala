import models.{IpEvent, IpStatistics}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SQLContext}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery}



object StreamGroupWithState extends SparkStreamsApp {
  //TODO if session expired => create new with default field and status inactive
  def updateUserStatistics(
                            id: Int,
                            newEvents: Iterator[IpEvent],
                            oldState: GroupState[IpStatistics]): IpStatistics = {
    var state=
    if (oldState.exists){
      oldState.get
    } else{
      val visit = newEvents.next()
      var newState = models.IpStatistics(id, Seq.empty, 0,visit.timestamp,visit.timestamp,0)
      newState = newState
        .copy(visits = newState.visits ++ Seq(visit),
              totalVisits = newState.totalVisits + 1)
      oldState.update(newState)
      newState
    }
    for (event <- newEvents) {
      state = state
        .copy(visits = state.visits ++ Seq(event),
          totalVisits = state.totalVisits + 1,
          totalTimeMS = event.timestamp.getTime-state.lastTime.getTime)
      oldState.update(state)
    }
    state
  }
  import spark.implicits._

  implicit val sqlCtx: SQLContext = spark.sqlContext

  implicit val pageVisitEncoder: Encoder[IpEvent] = Encoders.product[IpEvent]
  implicit val userStatisticsEncoder: Encoder[IpStatistics] = Encoders.product[IpStatistics]

  val visitsStream = MemoryStream[IpEvent]

  val pageVisitsTypedStream: Dataset[IpEvent] = visitsStream.toDS()

  var query: StreamingQuery = _
  query=pageVisitsTypedStream.groupByKey(_.ip)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateUserStatistics)
    .writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .option("checkpointLocation", checkpointLocation)
    .start()
  val gen = new GeneratorEvents
  //Initial Batch
  visitsStream.addData(gen.generate())
  query.processAllAvailable()
  /*
  -------------------------------------------
  Batch: 0
  -------------------------------------------
  +---+--------------------+-----------+--------------------+--------------------+-----------+
  | ip|              visits|totalVisits|          firstVisit|            lastTime|totalTimeMS|
  +---+--------------------+-----------+--------------------+--------------------+-----------+
  |  1|[[1, google.com/1...|          2|2020-01-23 01:16:...|2020-01-23 01:16:...|     1000.0|
  |  0|[[0, google.com/2...|          3|2020-01-23 01:16:...|2020-01-23 01:16:...|     4000.0|
  +---+--------------------+-----------+--------------------+--------------------+-----------+

  You can check checkpoint state: target/checkpoint-StreamGroupWithState/state
 */
  println(s"You can check checkpoint state: ${stateLocation}")
  pause()
  //Second Batch
  visitsStream.addData(gen.generate())
  /*
  -------------------------------------------
  Batch: 1
  -------------------------------------------
  +---+--------------------+-----------+--------------------+--------------------+-----------+
  | ip|              visits|totalVisits|          firstVisit|            lastTime|totalTimeMS|
  +---+--------------------+-----------+--------------------+--------------------+-----------+
  |  1|[[1, google.com/1...|          3|2020-01-23 01:16:...|2020-01-23 01:16:...|    53121.0|
  |  2|[[2, google.com/2...|          1|2020-01-23 01:17:...|2020-01-23 01:17:...|        0.0|
  |  0|[[0, google.com/2...|          6|2020-01-23 01:16:...|2020-01-23 01:16:...|    58121.0|
  +---+--------------------+-----------+--------------------+--------------------+-----------+
   */
  query.processAllAvailable()
  pause()
}
