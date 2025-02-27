import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object user_review_EL {
  def integrate_user_review(spark: SparkSession, cnxpsql: Properties, urlpsql: String, cnxoracle: Properties, urloracle: String): Unit = {
    var user = spark.read.jdbc(urlpsql, "(SELECT user_id, name, fans, yelping_since FROM \"user\") AS selected_users", cnxpsql)
    user.show()

    var timeQuery = "(SELECT * FROM \"TIME\")"
    var timeDF = spark.read.jdbc(urloracle, timeQuery, cnxoracle)
    val timeDF_date = timeDF.withColumn("time_id", col("time_id").cast("int"))
      .withColumn("year", col("year").cast("int"))
      .withColumn("month", col("month").cast("int"))
      .withColumn("day", col("day").cast("int"))
      .withColumn("date", concat_ws("-", col("year"), col("month"), col("day")).cast("date"))
      .drop("year", "month", "day")
//    timeDF_date.show()

    val joinedDF = user.join(timeDF_date, user("yelping_since") === timeDF_date("date"), "inner")

    val user_yelp = joinedDF
      .drop("yelping_since", "date")
      .withColumn("user_id", col("user_id").cast(StringType))
      .withColumn("name", col("name").cast(StringType))
      .withColumn("fans", col("fans").cast(IntegerType))
      .withColumn("yelping_since", col("time_id").cast(IntegerType))
      .drop("time_id")
//    user_yelp.show()
//    val rowCount_date = user_yelp.count()
//    val rowCount_date_user = user.count()
//    println(s"Total number of rows time: $rowCount_date")
//    println(s"Total number of rows user: $rowCount_date_user")

    // Enregistrement du DataFrame users dans la table "user"
    user_yelp.write
      .mode(SaveMode.Append).jdbc(urloracle, "user_yelp", cnxoracle)

  }
}
