package com.bits.twitter.streaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import com.bits.twitter.utils.Utilities._

object PrintTweets {
 
  def main(args: Array[String]) {

    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "Tweet Sentiment", Seconds(1))
    
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into RDD's using map()
    val tweetStatus = tweets.map(status => status.getText())
    
    // Print out the first ten
    tweetStatus.print()
    
    // start the context for twitter feeds
    ssc.start()
    ssc.awaitTermination()
  }  
}