import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.util.Properties

object time_review_EL {
  def integrate_time_review(spark: SparkSession,urlPostgres:String,cnxPostgres:Properties,urlKafka:String,cnxKafka:Properties): Unit = {


    //  Récupérer les dates de `review`
    val review_date = spark.read
      .jdbc(urlPostgres, "(SELECT DISTINCT date FROM review) AS selected_reviews", cnxPostgres)
      .withColumn("year", year(col("date").cast("date")))
      .withColumn("month", month(col("date").cast("date")))
      .withColumn("day", dayofmonth(col("date").cast("date")))
      .select("year", "month", "day")
      .distinct()

    //  Récupérer les dates existantes dans `time` (Oracle)
    val existing_time = spark.read
      .jdbc(urlKafka, "time", cnxKafka)
      .select("year", "month", "day")
      .distinct()


    // Filtrer pour ne garder que les nouvelles dates
    val new_dates = review_date.except(existing_time)

    val count_lines = new_dates.count()


    val existing_time_id = spark.read
      .jdbc(urlKafka, "time", cnxKafka)
      .select("time_id")

    val lastIndex = existing_time_id.agg(max("time_id")).collect()(0)(0) match {
      case null => 0 // Handle empty table by returning 0
      case value => value.toString.toInt
    }

    val windowSpec = Window.orderBy(lit(1)) // Order by a constant to ensure row_number assignment
    val new_dates_WithIndex = new_dates.withColumn("time_id", row_number().over(windowSpec) + lastIndex)

    // Insérer uniquement les nouvelles dates dans `time`
    new_dates_WithIndex
      .write
      .mode(SaveMode.Append)
      .jdbc(urlKafka, "time", cnxKafka)

    println(s"Nouvelles dates insérées avec succès dans la table time => $count_lines lignes insérées")
  }
}
