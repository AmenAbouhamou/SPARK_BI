import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object business_EL {
	def integrate_business(spark: SparkSession,shopFile: String,connectionProperties: Properties,url: String): Unit = {

		// Chargement du fichier JSON
		var shop = spark.read.json(shopFile).cache()
		// Changement du type d'une colonne
		shop = shop.withColumn("business_id", col("business_id").cast(StringType))

		// Affichage du schéma des DataFrame
		shop.printSchema()
		var shopn=shop.select("business_id","name","state","city","is_open")
		shopn.printSchema()
		shopn.show()

		// Enregistrement du DataFrame users dans la table "user"
		shopn.write
			.mode(SaveMode.Append).jdbc(url, "business", connectionProperties)

	}
}
