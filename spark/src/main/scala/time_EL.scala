
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.expressions.Window


import time_review_EL._
import time_tip_EL._

object time_EL {
  def integrate_time(spark: SparkSession, shopFile: String, shoptip: String, urlPostgres: String, cnxPostgres: Properties, urlKafka: String, cnxKafka: Properties): Unit = {

    // 📌 Charger les dates depuis PostgreSQL
    val user_date = spark.read
      .jdbc(urlPostgres, "yelp.user", cnxPostgres)
      .select("yelping_since")
      .distinct()

    val time_from_users = user_date
      .withColumn("year", year(col("yelping_since").cast("date")))
      .withColumn("month", month(col("yelping_since").cast("date")))
      .withColumn("day", dayofmonth(col("yelping_since").cast("date")))
      .select("year", "month", "day") // Harmonisation des noms
      .distinct()

    // 📌 Chargement du fichier JSON
    val shop = spark.read.json(shopFile).cache()

    // 📌 Transformation des dates
    val time_from_json = shop
      .withColumn("date", explode(split(col("date"), ",\\s*"))) // Séparer les dates
      .withColumn("date", trim(col("date"))) // Nettoyer
      .withColumn("year", year(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("month", month(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("day", dayofmonth(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
      .select("year", "month", "day") // Harmonisation des noms
      .distinct()

    // 📌 Fusion des deux sources sans doublons
    val time_data_df = time_from_users.union(time_from_json).distinct()
    val windowSpec = Window.orderBy(lit(1))
    val newDataWithIndex = time_data_df.withColumn("time_id", row_number().over(windowSpec))
      .select("time_id", "year", "month", "day")
    // 📌 Suppression des valeurs NULL avant insertion
    val clean_time_df = newDataWithIndex.filter("year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL")

    // 📌 Insertion des nouvelles dates dans la base Oracle
    clean_time_df
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(urlKafka, "time", cnxKafka)

    // Call the integrate method from time_tip_EL
    integrate_time_tip(spark, shoptip, urlKafka, cnxKafka)

    // Call the integrate method from time_review_EL
    integrate_time_review(spark,urlPostgres, cnxPostgres, urlKafka, cnxKafka)

    println("✅ Données insérées avec succès dans la table TIME !")

  }
}
