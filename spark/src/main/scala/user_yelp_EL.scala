import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object user_yelp_EL {
  def integrate_user_yelp(spark: SparkSession, cnxpsql: Properties, urlpsql: String, cnxkafka: Properties, urlkafka: String): Unit = {

    var user = spark
      .read
      .jdbc(urlpsql, "yelp.user", cnxpsql)
      .select("user_id", "name", "fans", "yelping_since")
//    user.show()

    var user_count = user.count()

    var timeDF = spark
      .read
      .jdbc(urlkafka, "TIME", cnxkafka)

    var timeDF_date = timeDF
      .withColumn("time_id", col("time_id").cast("int"))
      .withColumn("year", col("year").cast("int"))
      .withColumn("month", col("month").cast("int"))
      .withColumn("day", col("day").cast("int"))
      .withColumn("date", concat_ws("-", col("year"), col("month"), col("day")).cast(DateType))
      .drop("year", "month", "day")

    var joinedDF = user
      .join(timeDF_date, user("yelping_since") === timeDF_date("date"), "inner")

    var user_yelp = joinedDF
      .drop("yelping_since", "date")
      .withColumn("user_id", col("user_id").cast(StringType))
      .withColumn("name", col("name").cast(StringType))
      .withColumn("fans", col("fans").cast(IntegerType))
      .withColumn("yelping_since", col("time_id").cast(IntegerType))
      .drop("time_id")


    user_yelp
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(urlkafka, "user_yelp", cnxkafka)

    var user_yelp_count = user_yelp.count()

    println("user_yelp: " + user_yelp_count + " rows"+" \t user: "+user_count +" rows difference: "+(user_count-user_yelp_count))

  }
}
