import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import business_EL._
import category_EL._
import user_review_EL._
import user_review_date_EL._
import java.util.Properties

object SimpleApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

		// Paramètres de la connexion BD

		Class.forName("oracle.jdbc.OracleDriver")
		val urlOracle = "jdbc:oracle:thin:@stendhal:1521:ENSS2024"
		val cnxOracle = new Properties()
		cnxOracle.setProperty("driver", "oracle.jdbc.OracleDriver")
		cnxOracle.setProperty("user", "aa224325")
		cnxOracle.setProperty("password","aa224325")

		// PostgreSQL Connection
		Class.forName("org.postgresql.Driver")
		val urlPostgres = "jdbc:postgresql://stendhal.iem:5432/tpid2020" // Update host & DB name
		val cnxPostgres = new Properties()
		cnxPostgres.setProperty("driver", "org.postgresql.Driver")
		cnxPostgres.setProperty("user", "tpid")  // Update username
		cnxPostgres.setProperty("password", "tpid") // Update password
		cnxPostgres.setProperty("currentSchema", "yelp") // Update schema name

		val shopFile = "/home/spark/data/yelp_academic_dataset_business.json"
//			"/Virtuel/aa224325/BI/yelp_academic_dataset_business.json"


//		 // Call the integrate method from business_EL
//		 integrate_business(spark,shopFile,cnxOracle,urlOracle)

//		// Call the integrate method from category_EL
//		integrate_category(spark,shopFile,cnxOracle,urlOracle)

//		// Call the integrate method from user_review_date_EL
//		integrate_user_review_date(spark,cnxPostgres,urlPostgres,cnxOracle,urlOracle)

		// Call the integrate method from user_review_EL
		integrate_user_review(spark,cnxPostgres,urlPostgres,cnxOracle,urlOracle)



		// Arrêt de Spark
		spark.stop()

	}
}

/*
* table
* category (category_id,business_id, name) fait
* business (business_id, name, state, city, is_open) fait
* time (time_id, year, month, day) à completer par W et par rewiew_date
* user_yelp (user_id, name, fans, yelping_since) fait
* review_user (user_id, business_id, stars, review_date, review_text) fait par W
* review (review_id, stars, review_date, review_text) W
* tips (user_id, business_id, timeid,...) à faire
* */
