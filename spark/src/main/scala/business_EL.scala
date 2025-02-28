import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object business_EL {
	def integrate_business(spark: SparkSession,shopFile: String,cnxKafka: Properties,urlKafka: String): Unit = {

		var shop = spark.read.json(shopFile).cache()
		shop = shop
			.withColumn("city", trim( initcap( lower( regexp_replace( regexp_replace( regexp_replace(col("city"),",.*", ""), "[^a-zA-ZéèàôûâîÎœ'\\-]", " "),"\\s+"," " ) ) ) ) )
			.select("business_id","name","state","city","is_open")

		shop.printSchema()

		shop.write
			.mode(SaveMode.Overwrite).jdbc(urlKafka, "business", cnxKafka)

	}
}
