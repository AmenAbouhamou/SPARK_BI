import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object business_tip_ETL {
  def integrate_business_tip(spark: SparkSession, businessurl: String, checkinurl: String, tipurl: String, cnxKafka: Properties, urlKafka: String, cnxPostgres: Properties, urlPostgres: String): Unit = {

    //////////////////////////////////////////////// Time /////////////////////////////////////////////

    var timeDF = spark.read.jdbc(urlKafka, "time", cnxKafka)
    var timeDF_date = timeDF
      .withColumn("time_id", col("time_id").cast("int"))
      .withColumn("year", col("year").cast("int"))
      .withColumn("month", col("month").cast("int"))
      .withColumn("day", col("day").cast("int"))
      .withColumn("date_time", concat_ws("-", col("year"), col("month"), col("day")).cast(DateType))
      .drop("year", "month", "day")

    //////////////////////////////////////////////// Tips /////////////////////////////////////////////

    var shop = spark.read
      .option("header", "true") // First row contains column names
      .option("multiLine", "true") // Enable handling of multi-line text fields
      .csv(tipurl)
      .cache()

    var tip_selected = shop
      // Remove new lines inside text
      .withColumn("text", regexp_replace(col("text"), "[\\r\\n]+", " "))
      // Replace commas with semicolons only inside quoted strings
      .withColumn("text", regexp_replace(col("text"), ",", ";"))
      // Convert date to YYYY-MM-DD format
      .withColumn("date_time", concat_ws("-", year(col("date")), month(col("date")), dayofmonth(col("date"))).cast(DateType))
      .drop("date")

    var joinedDF = tip_selected.join(timeDF_date, Seq("date_time"), "left")

    var tips_yelp = joinedDF
      .withColumn("business_id", col("business_id").cast(StringType))
      .withColumn("compliment_count", col("compliment_count").cast(IntegerType))
      .withColumn("user_id", col("user_id").cast(StringType))
      .withColumn("time_id", col("time_id").cast(IntegerType))
      .drop("date_time","text")

    var tot_compliment = tips_yelp
      .groupBy("business_id", "user_id")
      .agg(count("compliment_count").alias("tot_compliment"))

    var tot_tips = tip_selected
      .groupBy("user_id")
      .agg(count("text").alias("tot_tip"))

    tips_yelp = tips_yelp
      .join(tot_compliment, Seq("business_id", "user_id"), "left")

    tips_yelp = tips_yelp
      .join(tot_tips, Seq("user_id"), "left")

    //////////////////////////////////////////////// Business /////////////////////////////////////////////

    var business_json = spark.read
      .json(businessurl)
      .cache()

    var business = business_json
      .withColumn("avg_stars", col("stars").cast(DoubleType))
      .select("business_id", "avg_stars")

    tips_yelp = tips_yelp.join(business, Seq("business_id"), "left")

    //////////////////////////////////////////////// Checkin /////////////////////////////////////////////

    var checkin_json = spark.read.json(checkinurl).cache()

    var checkin = checkin_json
      .withColumn("date_checkin", explode(split(col("date"), ",\\s*"))) // Séparer les dates

    var checkin_tot = checkin
      .groupBy("business_id")
      .agg(count("date_checkin").alias("tot_checkin"))

    tips_yelp = tips_yelp
      .join(checkin_tot, Seq("business_id"), "left")

    ///////////////////////////////////////////////// Review /////////////////////////////////////////////

    //  Charger `review` depuis PostgreSQL
    val reviewDF = spark.read
      .jdbc(urlPostgres, "review", cnxPostgres)
      .select(
        col("review_id").alias("review_id"),
        col("user_id").alias("user_id"),
        col("business_id").alias("business_id")
      )
      .distinct()

    val userReviewCountDF = reviewDF
      .groupBy("user_id", "business_id")
      .agg(count("review_id").alias("tot_review_business"))

    tips_yelp = tips_yelp
      .join(userReviewCountDF, Seq("business_id", "user_id"), "left")

    ///////////////////////////////////////////////// Sauvegarde /////////////////////////////////////////////

    tips_yelp = tips_yelp.na.fill(0, Seq("tot_tip", "tot_checkin", "tot_review_business", "tot_compliment"))

    // Sauvegarde en CSV
    tips_yelp.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", ",")
      .csv("./user_id_not_found_data")

    ///////////////////////////////////////////// Insert Database /////////////////////////////////////////////

    // Sauvegarde en PostgreSQL
    tips_yelp.write
      .mode(SaveMode.Overwrite).jdbc(urlKafka, "tip", cnxKafka)

  }
}
