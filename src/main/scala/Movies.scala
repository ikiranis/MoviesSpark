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

        // Διάβασμα του αρχείου csv και επεξεργασία των απαραίτητων στηλών
        val df = ss.read.option("header", "true").csv(inputFile)
            // Get year column from title in format (YYYY)
            .withColumn("year", regexp_replace(col("title"), ".*\\((\\d{4})\\).*", "$1"))
            // Get title column without year
            .withColumn("title", regexp_replace(col("title"), "\\s*\\(\\d{4}\\)", ""))
            // Split genres column by | and explode
            .withColumn("genres", explode(split(col("genres"), "\\|")))

        // Εμφάνιση των κατηγοριών και μέτρηση των ταινιών που ανήκουν σε κάθε μία
        df.groupBy("genres")
            .count()
            .orderBy(col("count").desc)
            .show()



//        df.show()
//        df.printSchema()
    }
}
