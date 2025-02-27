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
        col("review_id").alias("REVIEW_ID"),
        col("user_id").alias("USER_ID"),
        col("business_id").alias("BUSINESS_ID"),
        col("date").alias("DATE"),
        col("stars").alias("STARS")
      )
      .distinct()

    //  Charger `TIME` depuis Oracle pour récupérer `TIME_ID`
    val timeDF = spark.read
      .jdbc(urlKafka, "TIME", cnxKafka)
      .select("TIME_ID", "YEAR", "MONTH", "DAY")
      .withColumn("time_id",col("time_id").cast(IntegerType))

    //  Associer `review.DATE` avec `TIME_ID`
    val reviewWithTime_left = reviewDF
      .withColumn("YEAR", year(col("DATE")))
      .withColumn("MONTH", month(col("DATE")))
      .withColumn("DAY", dayofmonth(col("DATE")))
      .join(timeDF, Seq("YEAR", "MONTH", "DAY"), "left") // ⬅️ INNER JOIN pour s'assurer de la correspondance
      // ⬅️ Sélectionner les bonnes colonnes

    val timeIdNullDF = reviewWithTime_left.filter(col("TIME_ID").isNull)

    // Sauvegarde en CSV
    timeIdNullDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", ",")
      .csv("./time_id_not_found_data")

    //  Afficher les lignes avec `TIME_ID` nul
    val reviewWithTime = reviewWithTime_left.filter(col("TIME_ID").isNotNull).select("REVIEW_ID", "USER_ID", "BUSINESS_ID", "TIME_ID", "STARS")

    //  Afficher le schéma et les 5 premières lignes
    println("Schéma après jointure avec TIME :")
    reviewWithTime.printSchema()
    reviewWithTime.show(5)

    //  Charger et compter `NB_CATEGORIES` depuis `CATEGORY`
    val categoryDF = spark.read
      .jdbc(urlKafka, "CATEGORY", cnxKafka)
      .groupBy("BUSINESS_ID")
      .agg(count("*").alias("NB_CATEGORIES")) // Nombre de catégories par business

    //  Charger les check-ins (`NB_CONNEXION`) depuis le fichier JSON
    val checkinDF = spark.read.json(checkinpath)

    val businessCheckinsDF = checkinDF
      .withColumn("date", explode(split(col("date"), ",\\s*"))) // Séparer les dates de check-in
      .groupBy("business_id")
      .agg(count("date").alias("NB_CONNEXION")) // Compter le nombre de check-ins

    // Calculer `REVIEW_COUNT` (nombre total de reviews par user)
    val userReviewCountDF = reviewDF
      .groupBy("USER_ID")
      .agg(count("REVIEW_ID").alias("REVIEW_COUNT")) // Nombre de reviews par user

    // Associer `BUSINESS_ID` avec `NB_CATEGORIES` et `NB_CONNEXION`
    val reviewWithStats = reviewWithTime
      .join(categoryDF, Seq("BUSINESS_ID"), "inner") // Associer avec `CATEGORY`
      .join(businessCheckinsDF, Seq("BUSINESS_ID"), "inner") // Associer avec `check-in`
      .join(userReviewCountDF, Seq("USER_ID"), "inner") // Associer avec `REVIEW_COUNT`
      .na.fill(0, Seq("NB_CATEGORIES", "NB_CONNEXION", "REVIEW_COUNT")) // Remplace `null` par 0


      reviewWithStats.show()


    // println(" Schéma final avant insertion dans REVIEW :")
    // reviewWithStats.printSchema()
    // reviewWithStats.show(5)

    // //  Insérer les nouvelles `review` dans Oracle
    reviewWithStats.write
      .mode(SaveMode.Overwrite)
      .jdbc(urlKafka, "REVIEW", cnxKafka)



    // println("Données insérées avec succès dans la table REVIEW !")
  }
}
