package com.bits.twitter.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{ Durations, StreamingContext }
import com.bits.twitter.mllib.SentimentAnalyzer
import com.bits.twitter.utils._
import redis.clients.jedis.Jedis
import twitter4j.Status

import com.bits.twitter.utils.Utilities._
import redis.clients.jedis.JedisPool

/**
 * Analyzes and predicts Twitter Sentiment in [near] real-time using Spark Streaming and Spark MLlib.
 * Uses the Naive Bayes Model created from the Training data and applies it to predict the sentiment of tweets
 * collected in real-time with Spark Streaming, whose batch is set to 20 seconds [configurable].
 * Raw tweets [compressed] and also the gist of predicted tweets are saved to the disk.
 * At the end of the batch, the gist of predicted tweets is published to Redis.
 */

object StreamedTweetSentimentAnalyzer {

  def main(args: Array[String]) {
    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    /*    // Set up a Spark streaming context named "SaveTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
*/
    val ssc = createSparkStreamingContext
    val simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")

    // Load Naive Bayes Model from the location specified in the config file.
    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)
    val stopWordsList = ssc.sparkContext.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.stopWords))

    /**
     * Predicts the sentiment of the tweet passed.
     * @param status -- twitter4j.Status object.
     * @return tuple with Tweet ID, Tweet Text, MLlib Polarity, Latitude, Longitude, Profile Image URL, Tweet Date.
     */
    def predictSentiment(status: Status): (Long, String, String, Int, Double, Double, String, String) = {
      val tweetData = replaceNewLines(status.getText)
      val mllibSentiment = {
        // compute the sentiment by MLlib if only the language of the tweet is english
        if (isTweetInEnglish(status)) {

          SentimentAnalyzer.retrieveSentiment(tweetData, stopWordsList, naiveBayesModel)
         } else {
          // TODO: all non-English tweets are defaulted to neutral.
          // TODO: this is a workaround :: as we cant compute the sentiment of non-English tweets with our current model.
          0
        }
      }
        
      (
        status.getId,
        status.getUser.getScreenName,
        tweetData,
        mllibSentiment,
        status.getGeoLocation.getLatitude,
        status.getGeoLocation.getLongitude,
        status.getUser.getOriginalProfileImageURL,
        simpleDateFormat.format(status.getCreatedAt))
    }

    val rawTweets = TwitterUtils.createStream(ssc, None)

    // Save Raw tweets only if the flag is set to true.
    if (PropertiesLoader.saveRawTweets) {
      rawTweets.cache()

      rawTweets.foreachRDD { rdd =>
        if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
          saveRawTweetsInJSONFormat(rdd, PropertiesLoader.tweetsRawPath)
        }
      }
    }

    // This delimiter was chosen as the probability of this character appearing in tweets is very less.
    val DELIMITER = "¦"
    val tweetsClassifiedPath = PropertiesLoader.tweetsClassifiedPath
    val classifiedTweets = rawTweets.filter(hasGeoLocation).map(predictSentiment)

    classifiedTweets.foreachRDD { rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
        //saveClassifiedTweets(rdd, tweetsClassifiedPath)
         val repartitionedRDD = rdd.repartition(1).cache()
        // Now publish the data to Redis.
        repartitionedRDD.foreach {
          case (id, screenName, text, sent, lat, long, profileURL, date) => {
            val poolConfig = new JedisPool("localhost", 6379)
            val sentimentTuple = (id, screenName, text, sent, lat, long, profileURL, date)
            val jedis = poolConfig.getResource
            val write = sentimentTuple.productIterator.mkString(DELIMITER)
            jedis.sadd(screenName, write)

          }
        }
      }
    }

    ssc.start()
    //ssc.awaitTerminationOrTimeout(PropertiesLoader.totalRunTimeInMinutes * 60 * 1000) // auto-kill after processing rawTweets for n mins.
    ssc.awaitTermination()
  }

  /**
   * Create StreamingContext.
   * Future extension: enable checkpointing to HDFS [is it really required??].
   *
   * @return StreamingContext
   */
  def createSparkStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Use KryoSerializer for serializing objects as JavaSerializer is too slow.
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      // Reduce the RDD memory usage of Spark and improving GC behavior.
      .set("spark.streaming.unpersist", "true")
      //set the master as local node
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Durations.seconds(PropertiesLoader.microBatchTimeInSeconds))
    ssc
  }

  /**
   * Saves the classified tweets to the csv file.
   * Uses DataFrames to accomplish this task.
   *
   * @param rdd                  tuple with Tweet ID, Tweet Text, Core NLP Polarity, MLlib Polarity, Latitude, Longitude, Profile Image URL, Tweet Date.
   * @param tweetsClassifiedPath Location of saving the data.
   */
  def saveClassifiedTweets(rdd: RDD[(Long, String, String, Int, Double, Double, String, String)], tweetsClassifiedPath: String) = {
    val now = "%tY%<tm%<td%<tH%<tM%<tS" format new Date
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._
    val classifiedTweetsDF = rdd.toDF("ID", "ScreenName", "Text", "MLlib", "Latitude", "Longitude", "ProfileURL", "Date")
    classifiedTweetsDF.repartition(1).write
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(tweetsClassifiedPath + now)

  }

  /**
   * Jackson Object Mapper for mapping twitter4j.Status object to a String for saving raw tweet.
   */
  val jacksonObjectMapper: ObjectMapper = new ObjectMapper()

  /**
   * Saves raw tweets received from Twitter Streaming API in
   *
   * @param rdd           -- RDD of Status objects to save.
   * @param tweetsRawPath -- Path of the folder where raw tweets are saved.
   */
  def saveRawTweetsInJSONFormat(rdd: RDD[Status], tweetsRawPath: String): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val tweet = rdd.map(status => jacksonObjectMapper.writeValueAsString(status))
    import sqlContext.implicits._
   // val rawTweetsDF = sqlContext.read.json(tweet)
    val rawTweetDF = tweet.toDF()
    rawTweetDF.coalesce(1).write
      .format("json")
      .mode(SaveMode.Append)
      .save(tweetsRawPath)
  }

  /**
   * Removes all new lines from the text passed.
   *
   * @param tweetText -- Complete text of a tweet.
   * @return String without new lines.
   */
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  /**
   * Checks if the tweet Status and user language is in English language or not.
   *
   * @param status twitter4j Status object
   * @return Boolean status of tweet in English or not.
   */
  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en"
  }

  /**
   * Checks if the tweet Status has Geo-Coordinates.
   *
   * @param status twitter4j Status object
   * @return Boolean status of presence of Geolocation of the tweet.
   */
  def hasGeoLocation(status: Status): Boolean = {
    null != status.getGeoLocation
  }
}