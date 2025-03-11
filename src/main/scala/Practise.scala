import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lag, lead, lit, when}
import shaded.parquet.org.apache.thrift.Option.some

object Practise {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("Akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("CNH Industrial")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val gpsData = Seq(
      ("2025-03-11 10:00:00", "Vehicle1", Some(45.76), Some(-93.85)),
      ("2025-03-11 10:05:00", "Vehicle2", Some(46.01), Some(-94.02)),
      ("2025-03-11 10:10:00", "Vehicle3", Some(145.90), Some(-193.92)), // Outlier
      ("2025-03-11 10:15:00", "Vehicle1", Some(45.80), Some(-93.87)),
      ("2025-03-11 10:20:00", "Vehicle2", Some(46.05), Some(-94.10)),
      ("2025-03-11 10:25:00", "Vehicle3", Some(45.95), Some(-93.95)),
      ("2025-03-11 10:30:00", "Vehicle1", Some(45.85), Some(-93.89)),
      ("2025-03-11 10:35:00", "Vehicle2", Some(46.08), Some(-94.12)),
      ("2025-03-11 10:40:00", "Vehicle3",None,None), // Missing values
      ("2025-03-11 10:45:00", "Vehicle1", Some(45.88), Some(-93.91))
    ).toDF("timestamp", "vehicle_id", "latitude", "longitude")
    //gpsData.show()

    // Removing Outliers in GPS Data
    val gpsFiltered = gpsData.filter("latitude between 40 and 50 and longitude between -100 and -80")
    //gpsFiltered.show()

    // Fuel Data
    val fuelData = Seq(
      ("Vehicle1", "2025-03-11 10:00:00", Some(8.5)),
      ("Vehicle2", "2025-03-11 10:05:00", Some(9.1)),
      ("Vehicle3", "2025-03-11 10:10:00", None), // Missing value
      ("Vehicle1", "2025-03-11 10:15:00", Some(7.8)),
      ("Vehicle2", "2025-03-11 10:20:00", Some(8.9)),
      ("Vehicle3", "2025-03-11 10:25:00", Some(50.0)), // Outlier
      ("Vehicle1", "2025-03-11 10:30:00", Some(8.0)),
      ("Vehicle2", "2025-03-11 10:35:00", Some(9.2)),
      ("Vehicle3", "2025-03-11 10:40:00", Some(8.7)),
      ("Vehicle1", "2025-03-11 10:45:00", Some(8.2))
    ).toDF("vehicle_id", "timestamp", "fuel_consumption")

   // fuelData.show()
    // Handling missing values & removing outliers
  val FilteredFuelData = fuelData
    .na.fill(Map("fuel_consumption" ->0))
    .filter("fuel_consumption between 5 and 15")
    //FilteredFuelData.show()

    // Engine Performance Data
    val engineData = Seq(
      ("Vehicle1", "2025-03-11 10:00:00", 2500),
      ("Vehicle2", "2025-03-11 10:05:00", 2700),
      ("Vehicle3", "2025-03-11 10:10:00", 2600),
      ("Vehicle1", "2025-03-11 10:15:00", 2550),
      ("Vehicle2", "2025-03-11 10:20:00", 2650),
      ("Vehicle3", "2025-03-11 10:25:00", 2750),
      ("Vehicle1", "2025-03-11 10:30:00", 2580),
      ("Vehicle2", "2025-03-11 10:35:00", 2680),
      ("Vehicle3", "2025-03-11 10:40:00", 2620),
      ("Vehicle1", "2025-03-11 10:45:00", 2600)
    ).toDF("vehicle_id", "timestamp", "engine_rpm")

    //Joining 3 datasets

    val joinedDF = gpsFiltered.join(FilteredFuelData,Seq("timestamp","vehicle_id"),"left")
      .join(engineData,Seq("timestamp","vehicle_id"),"left")
    //joinedDF.show()

    val cleanedData = joinedDF.na.fill(Map("fuel_consumption" ->0.0))
    //cleanedData.show()

    //Adding MetaData

    val enrichedData = cleanedData.withColumn("data_source", lit("Telemetry"))

    //enrichedData.show()

    //9. Use Window Functions for Time-Based Analysis

    val windowSpec = Window.partitionBy("vehicle_id").orderBy("timestamp")
    val processedData = enrichedData
      .withColumn("Moving_avg_fuel",avg($"fuel_consumption").over(windowSpec))
      .withColumn("Lead_fuel",lead($"fuel_consumption",1).over(windowSpec))
      .withColumn("Lag_fuel",lag($"fuel_consumption",1).over(windowSpec))

    //10.Aggregate Fuel Consumption

    val aggregateFuelConsumption = processedData.groupBy("vehicle_id")
      .agg(avg($"fuel_consumption").alias("Average_Fuel"))

    processedData.show()
    //aggregateFuelConsumption.show()

    //Compute the average engine RPM per vehicle

    val avgEngineRPM = engineData.groupBy("vehicle_id")
      .agg(avg($"engine_rpm").alias("Average_Engine_RPM"))

    //avgEngineRPM.show()

    //    // Window Specification for Lead and Lag Operations

    val engineWithWindowOps = engineData.
      withColumn("Prev_RPM",lag("engine_rpm",1).over(windowSpec))
      .withColumn("Next_RPM",lag("engine_rpm",1).over(windowSpec))
      .withColumn("Moving_Avg_RPM",avg("engine_rpm").over(windowSpec))

    //engineWithWindowOps.show()

    //// Handling missing values with mean

    val avgfuel = fuelData.agg(avg("fuel_consumption")).first().getDouble(0)
    val fuelfilled = fuelData.na.fill(Map("fuel_consumption"->avgfuel))

    // Capping outliers using percentile values
    val quantiles = fuelfilled.stat.approxQuantile("fuel_consumption", Array(0.05, 0.95), 0.0)
    val fuelCleaned = fuelfilled.withColumn("fuel_consumption",
      when(col("fuel_consumption") > quantiles(1), quantiles(1))
        .when(col("fuel_consumption") < quantiles(0), quantiles(0))
        .otherwise(col("fuel_consumption")))

    fuelData.show()
    fuelfilled.show()
    fuelCleaned.show()

    spark.stop()








  }

}
