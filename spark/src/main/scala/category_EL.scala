import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object category_EL {
	def integrate_category(spark: SparkSession,shopFile: String,connectionProperties: Properties,url: String): Unit = {

		// Chargement du fichier JSON
		var shop = spark.read.json(shopFile).cache()

		var shopdf=shop.select("business_id","categories").toDF()

		// Changement du type d'une colonne
		var shop_category = shopdf
        .withColumn("category_business", explode(split(col("categories"), ", ")))
        .drop("categories")
				.distinct()

		// Affichage du schéma des DataFrame
		shop_category.printSchema()
		shop_category.show()

		// Enregistrement du DataFrame users dans la table "user"
		shop_category.write
			.mode(SaveMode.Append).jdbc(url, "category", connectionProperties)

	}
}


