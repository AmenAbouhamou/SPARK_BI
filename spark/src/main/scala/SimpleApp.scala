import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

// Import des propriétés
import java.util.Properties

// Import des fonctions ETL
import business_EL._
import business_tip_ETL._
import review_EL._
import category_EL._
import user_yelp_EL._

// Import des fonctions ETL Wissam
import time_EL._
import review_user_EL._

object SimpleApp {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = SparkSession.builder().appName("ETL").master("local[4]").getOrCreate();

    // Chemin d'accès aux fichiers

    var path_init: String = "/Virtuel/aa224325/BI/";
    //    var path_init = "/home/spark/data/";

    var shop: String = path_init.+("yelp_academic_dataset_business.json");
    var shopcheckin: String = path_init.+("yelp_academic_dataset_checkin.json");
    var shoptip: String = path_init.+("yelp_academic_dataset_tip.csv");

    // Connexion à la base de données

    Class.forName("org.postgresql.Driver");
    var urlPostgres: String = "jdbc:postgresql://stendhal.iem:5432/tpid2020";
    var cnxPostgres: Properties = new Properties();
    cnxPostgres.setProperty("driver", "org.postgresql.Driver");
    cnxPostgres.setProperty("user", "tpid");
    cnxPostgres.setProperty("password", "tpid");
    cnxPostgres.setProperty("currentSchema", "yelp");


    Class.forName("org.postgresql.Driver");
    var urlPostgres_kafka: String = "jdbc:postgresql://kafka.iem:5432/aa224325";
    var cnxPostgres_kafka: Properties = new Properties();
    cnxPostgres_kafka.setProperty("driver", "org.postgresql.Driver");
    cnxPostgres_kafka.setProperty("user", "aa224325");
    cnxPostgres_kafka.setProperty("password", "aa224325");

    // ETL Process (Extract, Transform, Load)

   // Call the integrate method from time_EL
   integrate_time(spark, shopcheckin, shoptip, urlPostgres, cnxPostgres, urlPostgres_kafka, cnxPostgres_kafka)

    // Call the integrate method from business_EL
    integrate_business(spark, shop, cnxPostgres_kafka, urlPostgres_kafka)

    // Call the integrate method from category_EL
    integrate_category(spark, shop, cnxPostgres_kafka, urlPostgres_kafka)

    // Call the integrate method from user_yelp_EL
    integrate_user_yelp(spark, cnxPostgres, urlPostgres, cnxPostgres_kafka, urlPostgres_kafka)

    // Call the integrate method from business_tip_EL
    integrate_business_tip(spark, shop, shopcheckin, shoptip, cnxPostgres_kafka, urlPostgres_kafka, cnxPostgres, urlPostgres)

    // Call the integrate method from review_EL
    integrate_review(spark, shopcheckin, urlPostgres, cnxPostgres, urlPostgres_kafka, cnxPostgres_kafka)
    
    // Call the integrate method from review_user_EL
    integrate_review_user(spark, urlPostgres, cnxPostgres, urlPostgres_kafka, cnxPostgres_kafka)

    // Arrêt de Spark
    spark.stop();

  };
}
