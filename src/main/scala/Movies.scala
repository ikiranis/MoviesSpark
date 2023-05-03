import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lower, regexp_replace, split}

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
            // Εξαγωγή του έτους από τον τίτλο
            .withColumn("year", regexp_replace(col("title"), ".*\\((\\d{4})\\).*", "$1"))
            // Μετατροπή του έτους σε ακέραιο. Αν αποτύχει η μετατροπή, το έτος θα γίνει null.
            // (Για την περίπτωση που δεν υπάρχει έτος στον τίτλο)
            .withColumn("year", col("year").cast("int"))
            // Αφαίρεση του έτους από τον τίτλο
            .withColumn("title", regexp_replace(col("title"), "\\s*\\(\\d{4}\\)", " "))

        // Εμφάνιση των κατηγοριών και μέτρηση των ταινιών που ανήκουν σε κάθε μία
        println("Number of movies per genre")

        // Σπάσιμο των κατηγοριών σε ξεχωριστές γραμμές
        df.withColumn("genre", explode(split(col("genres"), "\\|")))
            .groupBy("genre") // Ομαδοποίηση ανά κατηγορία
            .count()    // Μέτρηση των ταινιών ανά κατηγορία
            .orderBy(col("genre").asc)  // Ταξινόμηση κατά αλφαβητική σειρά
            .show()

        // Μέτρηση των ταινιών που έχουν βγει ανά έτος. Εμφάνιση των 10 πρώτων
        println("Number of movies per year. Best 10 years")

        df.groupBy("year") // Ομαδοποίηση ανά έτος
            .count()    // Μέτρηση των ταινιών ανά έτος
            .orderBy(col("count").desc) // Ταξινόμηση κατά φθίνουσα σειρά
            .show(10) // Εμφάνιση των 10 πρώτων

        // Εμφάνιση των 10 πιο συχνών λέξεων στους τίτλους των ταινιών
        println("Most common words in movie titles. Best 10 words")

        // Σπάσιμο των τίτλων σε ξεχωριστές λέξεις (μετατροπή τους σε πεζά)
        df.withColumn("word", explode(split(lower(col("title")), " ")))
            .groupBy("word")    // Ομαδοποίηση ανά λέξη
            .count()    // Μέτρηση των ταινιών ανά λέξη
            .filter(col("word").rlike("\\w{4,}"))   // Φιλτράρισμα των λέξεων με λιγότερους από 4 χαρακτήρες
            .orderBy(col("count").desc) // Ταξινόμηση κατά φθίνουσα σειρά
            .show(10)   // Εμφάνιση των 10 πρώτων
    }
}
