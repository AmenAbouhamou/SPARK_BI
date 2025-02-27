import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object user_review_date_EL {
  def integrate_user_review_date(spark: SparkSession, cnxpsql: Properties, urlpsql: String, cnxoracle: Properties, urloracle: String): Unit = {
    var user_date = spark.read.jdbc(urlpsql, "(SELECT yelping_since FROM \"user\" GROUP BY yelping_since) AS selected_users", cnxpsql)
    user_date.show()

    // Count the number of rows
    var rowCount = user_date.count()
    println(s"Total number of rows: $rowCount")

    var time = user_date
      .withColumn("year", year(col("yelping_since").cast("date")))
      .withColumn("month", month(col("yelping_since").cast("date")))
      .withColumn("day", dayofmonth(col("yelping_since").cast("date")))
      .drop("yelping_since")

    time.show()

    //     		 Enregistrement du DataFrame time dans la table "time"
    time
      .write
      .mode(SaveMode.Append).jdbc(urloracle, "time", cnxoracle)

  }
}
