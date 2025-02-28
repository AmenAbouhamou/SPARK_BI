import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object review_user_EL {
  def integrate_review_user(spark: SparkSession,urlPostgres: String,cnxPostgres: Properties,urlKafka: String,cnxKafka: Properties): Unit = {

    // Chargement de la table `review` depuis PostgreSQL
    val reviewDF = spark.read
      .jdbc(urlPostgres, "yelp.review", cnxPostgres)
      .select(
        col("review_id").alias("review_id"),  // Clé primaire
        col("text").alias("text"),
        col("useful").cast("int").alias("useful"),
        col("cool").cast("int").alias("cool"),
        col("funny").cast("int").alias("funny")
      )
      .distinct() // Évite les doublons potentiels
      .persist()

    // Vérification des données avant insertion
    reviewDF.show(10)

    // Insertion dans la table Oracle "REVIEW_USER"
    reviewDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(urlKafka, "review_user", cnxKafka)

    println("Données insérées avec succès dans REVIEW_USER !")
  }
}
