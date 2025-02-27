import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object business_EL {
	def integrate_business(spark: SparkSession,shopFile: String,connectionProperties: Properties,url: String): Unit = {

		var shop = spark.read.json(shopFile).cache()
		shop = shop.withColumn("business_id", col("business_id").cast(StringType))

		shop.printSchema()
		var shopn=shop.select("business_id","name","state","city","is_open")

		shopn.write
			.mode(SaveMode.Overwrite).jdbc(url, "business", connectionProperties)

	}
}
