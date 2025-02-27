import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object review_user_EL {
  def integrate_review_user(spark: SparkSession): Unit = {
    // Connexion PostgreSQL
    val pgUrl = "jdbc:postgresql://stendhal.iem:5432/tpid2020"
    val pgProps = new Properties()
    pgProps.setProperty("user", "tpid")
    pgProps.setProperty("password", "tpid")
    pgProps.setProperty("driver", "org.postgresql.Driver")

    // Chargement de la table `review` depuis PostgreSQL
    val reviewDF = spark.read
      .jdbc(pgUrl, "yelp.review", pgProps)
      .select(
        col("review_id").alias("REVIEW_ID"),  // Clé primaire
        col("text").alias("TEXT"),
        col("useful").cast("int").alias("USEFUL"),
        col("cool").cast("int").alias("COOL"),
        col("funny").cast("int").alias("FUNNY")
      )
      .distinct() // Évite les doublons potentiels
      .persist()

    // Vérification des données avant insertion
    reviewDF.show(10)

    // Connexion Oracle
  val urlKafka = "jdbc:postgresql://kafka.iem:5432/aa224325"
    val cnxKafka = new Properties()
    cnxKafka.setProperty("driver", "org.postgresql.Driver")
    cnxKafka.setProperty("user", "aa224325")
    cnxKafka.setProperty("password", "aa224325")

    // Insertion dans la table Oracle "REVIEW_USER"
    reviewDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(urlKafka, "REVIEW_USER", cnxKafka)

    println("Données insérées avec succès dans REVIEW_USER !")
  }
}
