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
        println("Number of movies per genre")

        df.withColumn("genre", explode(split(col("genres"), "\\|")))
            .groupBy("genre")
            .count()
            .orderBy(col("genre").asc)
            .show()

        // Μέτρηση των ταινιών που έχουν βγει ανά έτος. Εμφάνιση των 10 πρώτων
        println("Number of movies per year. Best 10 years")

        df.groupBy("year")
            .count()
            .orderBy(col("count").desc)
            .show(10)

        // Μέτρηση των λέξεων που υπάρχουν στους τίτλους των ταινιών
        // Εμφάνιση των 10 πρώτων
        // Αφαίρεση των λέξεων με μήκος μικρότερο των 4 χαρακτήρων
        println("Most common words in movie titles. Best 10 words")

        df.withColumn("word", explode(split(lower(col("title")), " ")))
            .groupBy("word")
            .count()
            .filter(col("word").rlike("\\w{4,}"))
            .orderBy(col("count").desc)
            .show(10)

//        df.show()
//        df.printSchema()
    }
}
