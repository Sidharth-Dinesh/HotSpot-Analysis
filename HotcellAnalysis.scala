package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  private val log = Logger.getLogger("CSE511-Hotcell-Analysis")

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv")
    .option("delimiter", ";")
    .option("header", "false")
    .load(pointPath);
    
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql(
      "select CalculateX(nyctaxitrips._c5)," +
      "CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips"
    )
    val newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    val COL_X = "x"
    val COL_Y = "y"
    val COL_Z = "z"
    val COL_COUNT = "count"
    val COL_SUM = "sum"
    val COL_NOAC = "num_of_adj_cells"
    val COL_GSCORE = "g_score"
    val FIRST_DF = "first_df"
    val SECOND_DF = "second_df"

    /**
     * Filter the coordinates to the area
     * under observation and count the number
     * of pickups for the given cell.
     */
    val selectedCellsCountDf = pickupInfo
      .filter(
        col(COL_X) >= minX and col(COL_X) <= maxX
          and col(COL_Y) >= minY and col(COL_Y) <= maxY
          and col(COL_Z) >= minZ and col(COL_Z) <= maxZ
      )
      .groupBy(COL_X, COL_Y, COL_Z)
      .count() // This is the aggregation step for the groupBy and not the "count" action on the [[DataFrame]].
      .cache()

    val (total, sqrTotal) = selectedCellsCountDf.select(COL_COUNT)
      .rdd
      .aggregate((0.0, 0.0))(
        (acc: (Double, Double), r) => {
          val curr = r.getLong(0).toDouble
          (
            acc._1 + curr,
            acc._2 + (curr * curr)
          )
        },
        (x: (Double, Double), y: (Double, Double)) => (
          x._1 + y._1,
          x._2 + y._2
        )
      )

    val mean = total / numCells
    val std = math.sqrt((sqrTotal / numCells) - (mean * mean))

    log.info(s"Mean: $mean")
    log.info(s"Standard Deviation: $std")

    /**
     * Cross join to get all the cells and the filter
     * the cells which are adjacent to the each other.
     * 
     * NOTE: This cross join operation is one of the expensive operation
     * which joins millions of cells together just to filter upto 26 surronding
     * cells. Future work can be done to index these cells and find out a way to
     * join only the necessary cells.
     */
    val adjacentDf = selectedCellsCountDf.as(FIRST_DF)
      .crossJoin(selectedCellsCountDf.as(SECOND_DF))
      .filter(
        HotcellUtils.is_adjacent_cube(
          col(s"$FIRST_DF.$COL_X"), col(s"$FIRST_DF.$COL_Y"),
          col(s"$FIRST_DF.$COL_Z"), col(s"$SECOND_DF.$COL_X"),
          col(s"$SECOND_DF.$COL_Y"), col(s"$SECOND_DF.$COL_Z")
        )
      )
      .select(
        col(s"$FIRST_DF.$COL_X"), col(s"$FIRST_DF.$COL_Y"),
        col(s"$FIRST_DF.$COL_Z"), col(s"$SECOND_DF.$COL_COUNT")
      )
      .groupBy(COL_X, COL_Y, COL_Z)
      .agg(sum(COL_COUNT) as COL_SUM, count(COL_COUNT) as COL_NOAC)

    /**
     * Sorting after repartition may not be required.
     * But we have added the line as we the order is maintained
     * when there is only one part file to write, and sorting
     * 50 elements is not computationally heavy in the given case.
     */
    val scoredDf = adjacentDf.withColumn(COL_GSCORE,
      HotcellUtils.g_score(mean, std, numCells)(col(COL_SUM), col(COL_NOAC)))
      .sort(desc(COL_GSCORE))
      .limit(50)
      .repartition(1)
      .sort(desc(COL_GSCORE))
      .select(COL_X, COL_Y, COL_Z, COL_GSCORE)

    scoredDf.select(COL_X, COL_Y, COL_Z)
  }



}
