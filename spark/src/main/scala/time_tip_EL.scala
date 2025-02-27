import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.util.Properties

object time_tip_EL {
  def integrate_time_tip(spark: SparkSession, tipurl: String, urlKafka: String, cnxKafka: Properties): Unit = {

    var shop_tip = spark.read
      .option("header", "true") // First row contains column names
      .option("multiLine", "true") // Enable handling of multi-line text fields
      .csv(tipurl)
      .cache()

    var shop_tip_date = shop_tip
      .withColumn("YEAR", year(col("date")).cast("int"))
      .withColumn("MONTH", month(col("date")).cast("int"))
      .withColumn("DAY", dayofmonth(col("date")).cast("int"))
      .select("YEAR", "MONTH", "DAY")
      .distinct()

    //  Récupérer les dates existantes dans `TIME` (Oracle)
    val existing_time = spark.read
      .jdbc(urlKafka, "TIME", cnxKafka)
      .select("YEAR", "MONTH", "DAY")
      .distinct()

    // Filtrer pour ne garder que les nouvelles dates
    val new_dates = shop_tip_date.except(existing_time)

    val count_lines = new_dates.count()

    val existing_time_id = spark.read
      .jdbc(urlKafka, "TIME", cnxKafka)
      .select("TIME_ID")

    val lastIndex = existing_time_id.agg(max("TIME_ID")).collect()(0)(0) match {
      case null => 0 // Handle empty table by returning 0
      case value => value.toString.toInt
    }

    val windowSpec = Window.orderBy(lit(1)) // Order by a constant to ensure row_number assignment
    val new_dates_WithIndex = new_dates.withColumn("TIME_ID", row_number().over(windowSpec) + lastIndex)

    // Insérer uniquement les nouvelles dates dans `TIME`
    new_dates_WithIndex
      .write
      .mode(SaveMode.Append)
      .jdbc(urlKafka, "TIME", cnxKafka)

    println(s"\n\nNouvelles dates insérées avec succès dans la table TIME => $count_lines lignes insérées\n\n")
  }
}
