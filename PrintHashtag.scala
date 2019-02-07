/*
Listens to Streams of tweets - Extract the HashTags - Convert to RDDs - Map to DataFarames - Save as SQL tables to HDFS - SetUp Checkpoints
*/

package spark.twee

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object PrintHashtag {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data

    val ssc = new StreamingContext("local[2]", "PopularHashtags", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())

    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    // Now eliminate anything that's not a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))

    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    // Print the top 10
    sortedResults.print

     // Process each RDD from each batch as it comes in
    sortedResults.foreachRDD((rdd, time) => {
      // So we'll demonstrate using SparkSQL in order to query each RDD
      // using SQL queries.

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton2.getInstance(rdd.sparkContext)
      import sqlContext.implicits._


      // SparkSQL can automatically create DataFrames from Scala "case classes".
      // We created the Record case class for this purpose.
      // So we'll convert each RDD of tuple data into an RDD of "Record"
      // objects, which in turn we can convert to a DataFrame using toDF()
      val requestsDataFrame = rdd.map(w => Records(w._1, w._2)).toDF()

      // Create a SQL table from this DataFrame
      requestsDataFrame.createOrReplaceTempView("requests")

      // Count up occurrences of each user agent in this RDD and print the results.
      // The powerful thing is that you can do any SQL you want here!
      // But remember it's only querying the data in this RDD, from this batch.
      val twitterDataFrame =
        sqlContext.sql("select hashtag, count(*) as total from requests group by hashtag")
      println(s"========= $time =========")
    //  twitterDataFrame.show()

      twitterDataFrame.coalesce(1).write.mode("append").parquet("hdfs://hadoop.dywhin.com:9000/user/dive_info/twittersample9.parquet")

      // If you want to dump data into an external database instead, check out the
      // org.apache.spark.sql.DataFrameWriter class! It can write dataframes via
      // jdbc and many other formats! You can use the "append" save mode to keep
      // adding data from each batch.
    })

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("/home/dive_info/SparkExample")
    ssc.start()
    //ssc.awaitTerminationOrTimeout(3000)
    ssc.awaitTermination()
    ssc.stop()

  }
}

/** Case class for converting RDD to DataFrame */
case class Records(hashtag: String, count: Int)

/** Lazily instantiated singleton instance of SQLContext
 *  (Straight from included examples in Spark)  */
object SQLContextSingleton2 {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
