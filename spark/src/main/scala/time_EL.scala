// import org.apache.spark.sql.{SaveMode, SparkSession}
// import org.apache.spark.sql.functions._
// import java.util.Properties

// object time_EL {
//   def integrate_time(spark: SparkSession): Unit = {
//     // 📌 Chargement des données JSON
//     val shopFile = "/Virtuel/wk060453/BIData/yelp_academic_dataset_checkin.json"
//     val shop = spark.read.json(shopFile).cache()

//     // 📌 Transformation des dates
//     val explodedDF = shop
//       .withColumn("date", explode(split(col("date"), ",\\s*"))) // Séparer les dates
//       .withColumn("date", trim(col("date")))                   // Supprimer les espaces

//     // 📌 Extraction des composantes de la date
//     val timeDF = explodedDF
//       .withColumn("YEAR", year(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
//       .withColumn("MONTH", month(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
//       .withColumn("DAY", dayofmonth(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
//       .select("YEAR", "MONTH", "DAY") // Sélectionner seulement les colonnes utiles
//       .distinct() // 🔥 Appliquer DISTINCT après toutes les transformations

//     // val nb = timeDF.count()
//     // println(s"Total unique: $nb")

//     // 📌 Sauvegarde en CSV
//     // timeDF.coalesce(1)
//     //   .write
//     //   .mode(SaveMode.Overwrite) 
//     //   .option("header", "true")  
//     //   .option("delimiter", ";")  
//     //   .csv("./time_data")

//     // println("✅ Fichier CSV unique généré avec succès !")

//     // 📌 Connexion Oracle  
//     val url = "jdbc:oracle:thin:@stendhal:1521:ENSS2024"
//     val connectionProperties = new Properties()
//     connectionProperties.setProperty("driver", "oracle.jdbc.OracleDriver")
//     connectionProperties.setProperty("user", "aa224325")
//     connectionProperties.setProperty("password", "aa224325")

//     // 📌 Écriture dans la base Oracle
//     timeDF.write.mode(SaveMode.Append).jdbc(url, "TIME", connectionProperties)
//     println("✅ Insertion réussie dans la table TIME !")
//   }
// }

import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.expressions.Window

object time_EL {
  def integrate_time(spark: SparkSession, shopFile: String): Unit = {


    // PostgreSQL Connection
    Class.forName("org.postgresql.Driver")
    val urlPostgres = "jdbc:postgresql://stendhal.iem:5432/tpid2020" // Update host & DB name
    val cnxPostgres = new Properties()
    cnxPostgres.setProperty("driver", "org.postgresql.Driver")
    cnxPostgres.setProperty("user", "tpid") // Update username
    cnxPostgres.setProperty("password", "tpid") // Update password
    cnxPostgres.setProperty("currentSchema", "yelp") // Update schema name


    val urlKafka = "jdbc:postgresql://kafka.iem:5432/aa224325"
    val cnxKafka = new Properties()
    cnxKafka.setProperty("driver", "org.postgresql.Driver")
    cnxKafka.setProperty("user", "aa224325")
    cnxKafka.setProperty("password", "aa224325")


    // 📌 Charger les dates depuis PostgreSQL
    val user_date = spark.read
      .jdbc(urlPostgres, "(SELECT DISTINCT yelping_since FROM \"user\") AS selected_users", cnxPostgres)

    val time_from_users = user_date
      .withColumn("YEAR", year(col("yelping_since").cast("date")))
      .withColumn("MONTH", month(col("yelping_since").cast("date")))
      .withColumn("DAY", dayofmonth(col("yelping_since").cast("date")))
      .select("YEAR", "MONTH", "DAY") // Harmonisation des noms
      .distinct()

    // 📌 Chargement du fichier JSON
    val shop = spark.read.json(shopFile).cache()

    // 📌 Transformation des dates
    val time_from_json = shop
      .withColumn("date", explode(split(col("date"), ",\\s*"))) // Séparer les dates
      .withColumn("date", trim(col("date"))) // Nettoyer
      .withColumn("YEAR", year(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("MONTH", month(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("DAY", dayofmonth(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
      .select("YEAR", "MONTH", "DAY") // Harmonisation des noms
      .distinct()

    // 📌 Fusion des deux sources sans doublons
    val time_data_df = time_from_users.union(time_from_json).distinct()
    val windowSpec = Window.orderBy(lit(1))
    val newDataWithIndex = time_data_df.withColumn("TIME_ID", row_number().over(windowSpec))
      .select("TIME_ID", "YEAR", "MONTH", "DAY")
    // 📌 Suppression des valeurs NULL avant insertion
    val clean_time_df = newDataWithIndex.filter("YEAR IS NOT NULL AND MONTH IS NOT NULL AND DAY IS NOT NULL")



    // 📌 Insertion des nouvelles dates dans la base Oracle
    clean_time_df
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(urlKafka, "time", cnxKafka)

    println("✅ Données insérées avec succès dans la table TIME !")

  }
}
