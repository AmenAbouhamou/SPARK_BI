import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.util.Properties

object time_review_EL {
  def integrate_time_review(spark: SparkSession): Unit = {

    // PostgreSQL Connection
    Class.forName("org.postgresql.Driver")
    val urlPostgres = "jdbc:postgresql://stendhal.iem:5432/tpid2020" // Update host & DB name
    val cnxPostgres = new Properties()
    cnxPostgres.setProperty("driver", "org.postgresql.Driver")
    cnxPostgres.setProperty("user", "tpid") // Update username
    cnxPostgres.setProperty("password", "tpid") // Update password
    cnxPostgres.setProperty("currentSchema", "yelp") // Update schema name


    //  Récupérer les dates de `review`
    val review_date = spark.read
      .jdbc(urlPostgres, "(SELECT DISTINCT date FROM review) AS selected_reviews", cnxPostgres)
      .withColumn("YEAR", year(col("date").cast("date")))
      .withColumn("MONTH", month(col("date").cast("date")))
      .withColumn("DAY", dayofmonth(col("date").cast("date")))
      .select("YEAR", "MONTH", "DAY")
      .distinct()


    val urlKafka = "jdbc:postgresql://kafka.iem:5432/aa224325"
    val cnxKafka = new Properties()
    cnxKafka.setProperty("driver", "org.postgresql.Driver")
    cnxKafka.setProperty("user", "aa224325")
    cnxKafka.setProperty("password", "aa224325")


    //  Récupérer les dates existantes dans `TIME` (Oracle)
    val existing_time = spark.read
      .jdbc(urlKafka, "TIME", cnxKafka)
      .select("YEAR", "MONTH", "DAY")
      .distinct()


    // Filtrer pour ne garder que les nouvelles dates
    val new_dates = review_date.except(existing_time)

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

    println(s"Nouvelles dates insérées avec succès dans la table TIME => $count_lines lignes insérées")
  }
}
