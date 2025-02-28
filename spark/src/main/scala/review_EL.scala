import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object review_EL {
  def integrate_review(spark: SparkSession,checkinpath:String,urlPostgres:String,cnxPostgres:Properties,urlKafka:String,cnxKafka:Properties): Unit = {

    //  Charger `review` depuis PostgreSQL
    val reviewDF = spark.read
      .jdbc(urlPostgres, "review", cnxPostgres)
      .select(
        col("review_id").alias("review_id"),
        col("user_id").alias("user_id"),
        col("business_id").alias("business_id"),
        col("date").alias("date"),
        col("stars").alias("stars")
      )
      .distinct()

    //  Charger `time` depuis Oracle pour récupérer `time_id`
    val timeDF = spark.read
      .jdbc(urlKafka, "time", cnxKafka)
      .select("time_id", "year", "month", "day")
      .withColumn("time_id",col("time_id").cast(IntegerType))

    //  Associer `review.date` avec `time_id`
    val reviewWithTime_left = reviewDF
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("day", dayofmonth(col("date")))
      .join(timeDF, Seq("year", "month", "day"), "left") // ⬅️ INNER JOIN pour s'assurer de la correspondance
      // ⬅️ Sélectionner les bonnes colonnes

    val timeIdNullDF = reviewWithTime_left.filter(col("time_id").isNull)

    // Sauvegarde en CSV
    timeIdNullDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", ",")
      .csv("./time_id_not_found_data")

    //  Afficher les lignes avec `time_id` nul
    val reviewWithTime = reviewWithTime_left.filter(col("time_id").isNotNull).select("review_id", "user_id", "business_id", "time_id", "stars")

    //  Afficher le schéma et les 5 premières lignes
    println("Schéma après jointure avec time :")
    reviewWithTime.printSchema()
    reviewWithTime.show(5)

    //  Charger et compter `nb_categories` depuis `category`
    val categoryDF = spark.read
      .jdbc(urlKafka, "category", cnxKafka)
      .groupBy("business_id")
      .agg(count("*").alias("nb_categories")) // Nombre de catégories par business

    //  Charger les check-ins (`nb_connexion`) depuis le fichier JSON
    val checkinDF = spark.read.json(checkinpath)

    val businessCheckinsDF = checkinDF
      .withColumn("date", explode(split(col("date"), ",\\s*"))) // Séparer les dates de check-in
      .groupBy("business_id")
      .agg(count("date").alias("nb_connexion")) // Compter le nombre de check-ins

    // Calculer `review_count` (nombre total de reviews par user)
    val userreviewCountDF = reviewDF
      .groupBy("user_id")
      .agg(count("review_id").alias("review_count")) // Nombre de reviews par user

    // Associer `business_id` avec `nb_categories` et `nb_connexion`
    val reviewWithStats = reviewWithTime
      .join(categoryDF, Seq("business_id"), "inner") // Associer avec `category`
      .join(businessCheckinsDF, Seq("business_id"), "inner") // Associer avec `check-in`
      .join(userreviewCountDF, Seq("user_id"), "inner") // Associer avec `review_count`
      .na.fill(0, Seq("nb_categories", "nb_connexion", "review_count")) // Remplace `null` par 0


      reviewWithStats.show()


    // println(" Schéma final avant insertion dans review :")
    // reviewWithStats.printSchema()
    // reviewWithStats.show(5)

    // //  Insérer les nouvelles `review` dans Oracle
    reviewWithStats.write
      .mode(SaveMode.Overwrite)
      .jdbc(urlKafka, "review", cnxKafka)



    // println("Données insérées avec succès dans la table review !")
  }
}
