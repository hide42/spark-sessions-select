package s7.continious

import java.time.LocalDateTime
import org.apache.spark.sql.streaming.Trigger
import s7.SparkStreamsApp

object ContinuousProcessingTest extends SparkStreamsApp {

  import spark.implicits._
  deleteCheckpointLocation()
  deleteCheckpointLocation()
  spark.sparkContext.setLogLevel("INFO")

  val numbers = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", "10")
    .option("numPartitions", "2")
    .load()
    .map(rateRow => {
      // imitate hard work
      val q = LocalDateTime.now();
      Thread.sleep(500L)
      q.toString
    })

  numbers.writeStream.outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "test-topic")
    .trigger(Trigger.Continuous("3 second"))
    .option("checkpointLocation", checkpointLocation)
    .start().awaitTermination(60000L)

  // A sample output of accumulated messages could be:
  //21/05/14 00:12:26 INFO ContinuousExecution: New epoch 1 is starting.
  //21/05/14 00:12:28 INFO ContinuousExecution: New epoch 2 is starting.
  //21/05/14 00:12:30 INFO ContinuousExecution: New epoch 3 is starting.
  //21/05/14 00:12:30 INFO ContinuousWriteRDD: Writer for partition 1 in epoch 0 is committing.
  //21/05/14 00:12:30 INFO ContinuousWriteRDD: Writer for partition 0 in epoch 0 is committing.
  //21/05/14 00:12:30 INFO ContinuousWriteRDD: Writer for partition 0 in epoch 0 committed.
  //21/05/14 00:12:30 INFO ContinuousWriteRDD: Writer for partition 1 in epoch 0 committed.
  //21/05/14 00:12:32 INFO ContinuousExecution: New epoch 4 is starting.
  //21/05/14 00:12:32 INFO ContinuousWriteRDD: Writer for partition 1 in epoch 1 is committing.
  //21/05/14 00:12:32 INFO ContinuousWriteRDD: Writer for partition 1 in epoch 1 committed.
  //21/05/14 00:12:32 INFO ContinuousWriteRDD: Writer for partition 0 in epoch 1 is committing.
  //21/05/14 00:12:32 INFO ContinuousWriteRDD: Writer for partition 0 in epoch 1 committed.
  // As you can see, the epochs start according to the defined trigger and not after committing given epoch.
  // Even if given executor is in late for the processing, it polls the information about the epochs regularly through
  // org.apache.spark.sql.execution.streaming.continuous.EpochPollRunnable


}
