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
            // Get year column from title in format (YYYY). If year not found (is number with 4 digits), set to 0
            .withColumn("year", regexp_replace(col("title"), ".*\\((\\d{4})\\).*", "$1"))
            .filter(col("year").cast("int") > 1900) // Fix bad year values
            // Get title column without year
            .withColumn("title", regexp_replace(col("title"), "\\s*\\(\\d{4}\\)", ""))


        // Εμφάνιση των κατηγοριών και μέτρηση των ταινιών που ανήκουν σε κάθε μία
        df.withColumn("genres", explode(split(col("genres"), "\\|")))
            .groupBy("genres")
            .count()
            .orderBy(col("genres").asc)
            .show()

        // Μέτρηση των ταινιών που έχουν βγει ανά έτος. Εμφάνιση των 10 πρώτων
        df.groupBy("year")
            .count()
            .orderBy(col("count").desc)
            .show(10)


        df.show()
//        df.printSchema()
    }
}
