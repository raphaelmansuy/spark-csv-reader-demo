import zio._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class CSVOptions (
  header: Boolean = true,
  inferSchema: Boolean = true,
  delimiter: String = ",",
  quote: String = "\"",
  escape: String = "\\",
  nullValue: String = "",
  dateFormat: String = "yyyy-MM-dd",
  timestampFormat: String = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
  multiLine: Boolean = false,
  ignoreLeadingWhiteSpace: Boolean = true,
  ignoreTrailingWhiteSpace: Boolean = true,
  charset: String = "UTF-8",
  comment: String = "#",
  emptyValue: String = "",
  nanValue: String = "NaN",
  positiveInf: String = "Inf",
  negativeInf: String = "-Inf",
  mode: String = "PERMISSIVE",
  columnNameOfCorruptRecord: String = "_corrupt_record",
  multiLineSeparator: String = "\n",
  maxColumns: Int = 20480,
  maxCharsPerColumn: Int = 10000,
  maxMalformedLogPerPartition: Int = 10,
  maxRecordPerFile: Int = 10000000,
  enforceSchema: Boolean = false,
  locale: String = "en-US",
  timeZone: String = "UTC")
{
  def toMap: Map[String,String] = {
    import scala.reflect.runtime.universe._
    // get the type of the case class
    val tpe = typeOf[this.type]

    // get all fields of the case class
    val fields = tpe.members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
    }

    // convert the fields to a map
    fields.map { field =>
      val name = field.name.toString
      val value = this.getClass.getDeclaredMethod(name).invoke(this).toString
      name -> value
    }.toMap
  }

}

// Service Definition
trait Logging {
  def log(line: String): UIO[Unit]
}

// Live implementation of Logging service
class LoggingLive extends Logging {
  override def log(line: String): UIO[Unit] =
    for {
      current <- Clock.currentDateTime
      _ <- Console.printLine(s"$current -- $line").orDie
    } yield ()
}

// Companion object of LoggingLive containing the service implementation into the ZLayer
object LoggingLive {
  val layer: URLayer[Any, Logging] =
    ZLayer.succeed(new LoggingLive)
}


trait SparkService {
  def getSparkSession: ZIO[Any,Throwable,SparkSession]
}

class SparkServiceLive extends SparkService {
  override def getSparkSession: ZIO[Any,Throwable,SparkSession] =
    ZIO.attempt(
      SparkSession.builder().appName("SparkSessionZioExample")
        .master("local[*]")
        .getOrCreate()
    )
}

object SparkServiceLive {
  val layer: URLayer[Any, SparkService] =
    ZLayer.succeed(new SparkServiceLive)
}

trait ReadCSV {
  def readCSV(path: String,csvOptions: CSVOptions = CSVOptions(),schema: Option[StructType] = None): ZIO[SparkService,Throwable,DataFrame]
}

// Live implementation of ReadCSV service using SparkService
class ReadCSVLive extends ReadCSV {
  override def readCSV(path: String,
             csvOptions: CSVOptions,
             schema: Option[StructType]): ZIO[SparkService,Throwable,DataFrame] =
    for {
      sparkService <- ZIO.service[SparkService]
      session <- sparkService.getSparkSession     
      schemaWithCorruptRecord = schema.map(s => s.add(StructField(csvOptions.columnNameOfCorruptRecord, StringType, true)))
      // if schema is defined, use it, otherwise use the schema inferred from the csv file
      df <- ZIO.succeed(session
                .read
                .option("header", "true")
                .option("mode", "PERMISSIVE")  // Allow malformed records
                .option("columnNameOfCorruptRecord", "_corrupt_record")  // Put malformed records in a new column
                .schema(schemaWithCorruptRecord.getOrElse(null))
                .csv(path))
    } yield df
}


object ReadCSVLive {
  val layer: URLayer[Any, ReadCSV] =
    ZLayer.succeed(new ReadCSVLive)
}

object MyApp extends ZIOAppDefault {

  val schema = StructType(
    List(
      StructField("sepallength", DoubleType, true),
      StructField("sepalwidth", DoubleType, true),
      StructField("petallength", DoubleType, true),
      StructField("petalwidth", DoubleType, true),
      StructField("class", StringType, true)
    )
  )

   val myApp = for {

    logger <- ZIO.service[Logging]
    _ <- logger.log("Starting Program")
    
    sparkService <- ZIO.service[SparkService]
    session <- sparkService.getSparkSession

    
    reader <- ZIO.service[ReadCSV]

    options = CSVOptions(delimiter = ",", header = true, inferSchema = false,  mode = "PERMISSIVE")

    df <- {
          reader.readCSV("src/main/resources/iris_csv.csv", options, Some(schema))
    }

    _ <- logger.log("Dataframe created")

    _ <- ZIO.succeed(df.printSchema())

    // Filter malformed records
    _ <- logger.log("Get malformed records")
    // find the line number of the malformed records




    dfMalformed <- ZIO.succeed(df.filter(col("_corrupt_record").isNotNull))

    // Print malformed records
    _ <- logger.log("Printing malformed records:")
    _ <- ZIO.succeed(dfMalformed.show())

    // Print count for the dataframe
    _ <- logger.log("Counting rows:")
    _ <- ZIO.succeed(df.count())

    _ <- ZIO.succeed(df.groupBy("sepalWidth").count().show())
    
    _ <- ZIO.succeed(session.stop())
    _ <- logger.log("Program Finished")
     
  } yield ()



  def run =
    myApp.provide(
      LoggingLive.layer,
      SparkServiceLive.layer,
      ReadCSVLive.layer
    )
}