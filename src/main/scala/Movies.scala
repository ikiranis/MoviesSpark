import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, first, lower, regexp_replace, split}

object Movies {
    def main(args: Array[String]): Unit = {
        val ss = SparkSession
            .builder()
            .appName("AirTweets")
            .config("spark.master", "local")
            .getOrCreate()

        val inputFile = "input/movies.csv"

        // Διάβασμα του αρχείου csv
        val df = ss.read.option("header", "true").csv(inputFile)
            // Αφαίρεση σημείων στίξης από τη στήλη text. Μετατροπή όλων των χαρακτήρων σε πεζά.
            .withColumn("title", regexp_replace(col("title"), "[^A-Za-z0-9]+", " "))
            .withColumn("title", lower(col("title")))

        df.show()
        df.printSchema()
    }
}
