import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.{FileSystems, Files}
import com.typesafe.scalalogging.LazyLogging

trait SparkStreamsApp extends App with LazyLogging{

  val appName = this.getClass.getSimpleName.replace("$", "")
  val queryName = appName
  val rootDir = "target"
  val checkpointLocation = s"$rootDir/checkpoint-$queryName"
  val stateLocation = s"$checkpointLocation/state"
  val master = "local[*]"
  val warehouseDir = s"$rootDir/$queryName-warehouse"

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder
    .master(master)
    .appName(appName)
    .config("spark.sql.warehouse.dir", warehouseDir)
    .getOrCreate
  spark.sparkContext.setLogLevel("WARN")

  deleteCheckpointLocation()

  def deleteCheckpointLocation(): Unit = {
    println(s">>> Deleting checkpoint location: $checkpointLocation")
    import scala.collection.JavaConverters._
    val path = FileSystems.getDefault.getPath(checkpointLocation)
    if (Files.exists(path)) {
      Files.walk(path)
        .iterator
        .asScala
        .foreach(p => p.toFile.delete)
    }
  }

  def pause() = {
    println(" >>> Pause processing")
    val webUrl = spark.sparkContext.uiWebUrl.get
    println(s"      Web UI @ $webUrl")
    println("      Press ENTER to continue...")
    val input = new BufferedReader(new InputStreamReader(System.in))
    input.readLine()
  }
}
