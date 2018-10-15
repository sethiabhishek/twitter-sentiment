package com.bits.twitter.streaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import com.bits.twitter.utils.Utilities._

/** This object aims to listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object PrintPopularHashtags {
  
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context 
    val ssc = new StreamingContext("local[*]", "PrintPopularHashtags", Seconds(1))
    
    setupLogging()

    // Create a DStream from Twitter
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Get the status DStream from the feed received
    val statuses = tweets.map(status => status.getText())
    
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    // Filter out hashtags from the words
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    
    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    
    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    
    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Print the top 10
    sortedResults.print
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
