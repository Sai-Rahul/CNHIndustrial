import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, broadcast, lag, lead, lit, when}
import org.apache.spark.storage.StorageLevel

object OptimizationTechniques {

  Logger.getLogger("Akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args:Array[String]) : Unit = {

    val spark=SparkSession.builder()
      .appName("CNH_Optimization_Techniques")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions","8")
      .getOrCreate()

    import spark.implicits._

    val gpsData = Seq(
      ("2025-03-11 10:00:00", "Vehicle1", Some(45.76), Some(-93.85)),
      ("2025-03-11 10:10:00", "Vehicle3", Some(145.90), Some(-193.92)), // Outlier
      ("2025-03-11 10:40:00", "Vehicle3", None, None), // Missing values
      ("2025-03-11 10:20:00", "Vehicle2", Some(46.05), Some(-94.10)),
      ("2025-03-11 10:25:00", "Vehicle3", Some(45.95), Some(-93.95)),
      ("2025-03-11 10:30:00", "Vehicle1", Some(45.85), Some(-93.89))
    ).toDF("timestamp", "vehicle_id", "latitude", "longitude")
      .select($"timestamp", $"vehicle_id",$"latitude".cast("double"), $"longitude".cast("double")) // Column Pruning

    //gpsData.show()
    //2. Filter Outliers Early (Predicate Pushdown)
    val gpsFiltered = gpsData.filter($"latitude".between(40,50)&& $"longitude".between(-100,- 80))
    //gpsFiltered.show()

    //3. Persist the Filtered Data (Avoid Recomputations)

    gpsFiltered.persist(StorageLevel.MEMORY_AND_DISK)
    //Persists (caches) the gpsFiltered dataset so it doesn’t need to be recomputed when reused.
    //MEMORY_AND_DISK ensures that if the dataset is too large for memory, Spark will store parts of it on disk.

    //4.Fuel Data with Outlier Handling

    val fuelData = Seq(
      ("Vehicle1", "2025-03-11 10:00:00", Some(8.5)),
      ("Vehicle3", "2025-03-11 10:10:00", None), // Missing value
      ("Vehicle3", "2025-03-11 10:25:00", Some(50.0)) // Outlier
    ).toDF("vehicle_id", "timestamp", "fuel_consumption")
      .select($"vehicle_id",$"timestamp",$"fuel_consumption".cast("double"))

    //5. Fill Missing Values with Mean & Cap Outliers

    val avgFuelConsumption = fuelData.agg(avg($"fuel_consumption")).first().getDouble(0)
    val quantiles = fuelData.stat.approxQuantile("fuel_consumption",Array(0.05,0.95),0.0)

    val fuelCleaned = fuelData.na.fill(Map("fuel_consumption"-> avgFuelConsumption)) //Filling Missing values with Average
      .withColumn("fuel_consumption",when($"fuel_consumption">quantiles(1),quantiles(1))
      .when($"fuel_consumption" < quantiles(0),quantiles(0))
        .otherwise($"fuel_consumption")
      )
    //✅ approxQuantile → Computes 5th and 95th percentiles to detect outliers.
    //✅ Capping → Ensures extreme values don’t distort the analysis.

    //fuelCleaned.show()

    val engineData = Seq(
      ("Vehicle1", "2025-03-11 10:00:00", 2500),
      ("Vehicle2", "2025-03-11 10:05:00", 2700)
    ).toDF("vehicle_id", "timestamp", "engine_rpm")
      .select($"vehicle_id", $"timestamp", $"engine_rpm".cast("int"))

    //engineData.show()

    //7.Optimize Join with Broadcast


    val joinedData = gpsFiltered
      .join(broadcast(fuelCleaned), Seq("vehicle_id", "timestamp"), "left") // Use broadcast for smaller table
      .join(engineData, Seq("vehicle_id", "timestamp"), "left")

    //✅ Broadcast Join → When one table (e.g., fuelCleaned) is small, Spark distributes it to all worker nodes to avoid expensive shuffles.
    //✅ Left Join → Ensures that vehicles without fuel/engine data are not lost.

    //joinedData.show()

    //8.  Add Metadata Column

    val enrichedData = joinedData.withColumn("datasource",lit("Telemetry"))
    //enrichedData.show()
    //✅ Adds a static column to indicate the data source.

    // 9. Window Functions for Moving Averages

    val windowSpec = Window.partitionBy("vehicle_id").orderBy("timestamp")

    val processedData = enrichedData
      .withColumn("MovingAverageFuelConsumption",avg($"fuel_consumption").over(windowSpec))
      .withColumn("Lead_Fuel",lead($"fuel_consumption",1).over(windowSpec))
      .withColumn("Lag_Fuel",lag($"fuel_consumption",1).over(windowSpec))

    //processedData.show()

    //10. Aggregate for Summary Stats

    val aggregationDF = processedData.groupBy("vehicle_id")
      .agg(avg($"fuel_consumption").alias("Avg_Fuel"))

    //aggregationDF.show()

    //11. Repartition Before Writing (Avoid Small Files)
    //Optimize Data Writing (Avoid Small Files)

    processedData.repartition(1).write.mode("overwrite").parquet("output/processed_data")
    aggregationDF.repartition(1).write.mode("overwrite").parquet("output/aggregated_data")
  //12.Display results
    processedData.show()
    aggregationDF.show()

    //13. Clean Up
  //✅ Releases cached data (unpersist()).
    gpsFiltered.unpersist()
    spark.stop()











  }

}
