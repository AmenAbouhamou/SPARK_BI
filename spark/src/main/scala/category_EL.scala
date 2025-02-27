import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object category_EL {
	def integrate_category(spark: SparkSession,shopFile: String,connectionProperties: Properties,url: String): Unit = {

		var shop = spark.read.json(shopFile).cache()

		var shopdf=shop.select("business_id","categories").toDF()

		var shop_category = shopdf
        .withColumn("category_business", explode(split(col("categories"), ", ")))
        .drop("categories")
				.distinct()

		shop_category.printSchema()
		shop_category.show()

		shop_category.write
			.mode(SaveMode.Overwrite).jdbc(url, "category", connectionProperties)

	}
}


