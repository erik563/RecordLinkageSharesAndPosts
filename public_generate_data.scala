// Databricks notebook source
//This code creates synthetic datasets. See section 3.5 of Erik's thesis (on record linkage for social media) on how this process works. 
import spark.implicits._
import sqlContext.implicits._

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import scala.math.{max,min, floor}

import collection.JavaConversions._
import java.time.{ZonedDateTime, ZoneId, LocalDate, LocalTime}


//Load datasets: All Share intents (dfEvents) and Post actions (dfTweets)
val dfTweets = spark.read.parquet("s3://persgroep/ad/tweets"); //input your own s3 bucket here
val dfEvents = spark.read.parquet("s3://persgroep/ad/events");


//Some UDFs for data cleaning and stuff
val browser_type_events = udf {
  (eventDev:String, eventOS: String) => (eventDev, eventOS) match {
    case (_, "Android") => 1 //This inlcudes native Samsung Browser - but excludes Opera Browser
    case ("Tablet", "iOS") => 2 //Tablet iOS
    case ("Mobile", "iOS") => 3 //Mobile iOS
    case (_, _) => 4 //Catch all tweets from Browsers
  }  
}

val browser_tweet = udf {
  (tweetSource: String) => (tweetSource) match {
    case ("Twitter for Android") => 1 //This inlcudes native Samsung Browser - but excludes Opera Browser
    case ("Twitter for iPhone") => 2 //For Tweets from iOS where Twitter is installed
    case ("Twitter for iPad") => 3 //For Tweets from iOS where Twitter is installed
    case ("Twitter Web Client") => 4 //Catch all tweets from Browsers
    case (_) => 5 //If not any cases of the above, it's not a match with our dataset.
  }
}

val ranges_of_5_seconds = udf {(x: Long) => 
  x-x%5
}

val addition = udf { (x: Long, y: Int ) =>
  x + y
}

val subtraction = udf {(x: Long, y: Long) => 
  y - x
}

val remove_false_matches = udf {(possible_matches: Int, avg_false_matches:Int) => 
  max(possible_matches-avg_false_matches, 0)
}

//Clean URLs -> removes stuff like: ?session=x1%20id=123
val clean_url = udf {(url: String) => 
  if (url.contains("?")) url.slice( 0, url.indexOf("?") )
  else url
}


//Generate random numbers between 1 and the number of records
val count_events = dfEvents.count();

val dfEventsNew = dfEvents
  .withColumn( "browser_category", browser_type_events($"dvce_type", $"os_family") ) //Split the devices into 4 categories
  .withColumn( "page_url", clean_url($"page_url") )
  .withColumn( "rand_nr_device", lit(round(rand()*count_events))) //Generate a random number, which is going to decide which device type this record is going to get.
  .withColumn( "rand_nr_url", lit(round(rand()*count_events))) //idem, but for the URL
  .withColumn( "rand_nr_userid", lit(round(rand()*count_events))) //idem, but for the userid 
  .withColumn( "rand_nr_city", lit(round(rand()*count_events))) //idem, but for the geo_city    
  .select("rn", "domain_userid", "page_url", "unix_tstamp", "browser_category", "geo_city", "rand_nr_device", "rand_nr_url", "rand_nr_userid", "rand_nr_city")
  .na.fill("No City",Seq("geo_city"));


//Per identifier, create a frequency table with a cumulative count

//Device type
val devices_grouped = dfEventsNew
  .groupBy("browser_category")
  .agg(count($"browser_category") as "count_browser_category")
  .orderBy("browser_category")
  .withColumn("cumulative_count", sum($"count_browser_category").over(Window.orderBy(asc("browser_category"))) ) //Cumulative count of the current record
  .withColumn("prev_cumulative_count", lag($"cumulative_count", 1, 0).over(Window.orderBy(asc("browser_category"))) ) //Cumulative count of the previous record
  .withColumnRenamed("browser_category", "synth_browser_category");

//Article location (URL)
val url_grouped = dfEventsNew
  .groupBy("page_url")
  .agg(count($"page_url") as "count_url")
  .orderBy("page_url")
  .withColumn("cumulative_count", sum($"count_url").over(Window.orderBy(asc("page_url"))) )
  .withColumn("prev_cumulative_count", lag($"cumulative_count", 1, 0).over(Window.orderBy(asc("page_url"))) )
  .withColumnRenamed("page_url", "synth_page_url");

//Entity identifier (cookie ID)
val ids_grouped = dfEventsNew
  .groupBy("domain_userid")
  .agg(count($"domain_userid") as "count_ids")
  .orderBy("domain_userid")
  .withColumn("cumulative_count", sum($"count_ids").over(Window.orderBy(asc("domain_userid"))) )
  .withColumn("prev_cumulative_count", lag($"cumulative_count", 1, 0).over(Window.orderBy(asc("domain_userid"))) )
  .withColumnRenamed("domain_userid", "synth_domain_userid");

//Location
val cities_grouped = dfEventsNew
  .groupBy("geo_city")
  .agg(count($"geo_city") as "count_cities")
  .orderBy("geo_city")
  .withColumn("cumulative_count", sum($"count_cities").over(Window.orderBy(asc("geo_city"))) )
  .withColumn("prev_cumulative_count", lag($"cumulative_count", 1, 0).over(Window.orderBy(asc("geo_city"))) )
  .withColumnRenamed("geo_city", "synth_geo_city");


//For each identifier, perform a join, where the random_number is less than the cumulative_count and more than the lag(cumulative_count,1).
val dfSynthEvents = dfEventsNew
  .join(devices_grouped, 
     dfEventsNew.col("rand_nr_device") <= devices_grouped.col("cumulative_count")  
     && dfEventsNew.col("rand_nr_device") > devices_grouped.col("prev_cumulative_count")  
  )
  .drop("count_browser_category", "cumulative_count", "prev_cumulative_count", "rand_nr_device")
 .join(urls_grouped, 
     dfEventsNew.col("rand_nr_url") <= urls_grouped.col("cumulative_count")  
     && dfEventsNew.col("rand_nr_url") > urls_grouped.col("prev_cumulative_count")
  )
  .drop("count_url", "cumulative_count", "prev_cumulative_count", "rand_nr_url")
  .join(ids_grouped, 
     dfEventsNew.col("rand_nr_userid") <= ids_grouped.col("cumulative_count")  
     && dfEventsNew.col("rand_nr_userid") > ids_grouped.col("prev_cumulative_count")
  )
  .drop("count_ids", "cumulative_count", "prev_cumulative_count", "rand_nr_userid")
  .join(cities_grouped, 
     dfEventsNew.col("rand_nr_city") <= cities_grouped.col("cumulative_count")  
     && dfEventsNew.col("rand_nr_city") > cities_grouped.col("prev_cumulative_count")
  )
  .drop("count_cities", "cumulative_count", "prev_cumulative_count", "rand_nr_city", "rn")
  .withColumn( "rn", row_number.over( Window.orderBy("unix_tstamp") ) ) //Add a new rownumber, because the old one may now miss a couple of numbers
  .select("rn", "unix_tstamp", "synth_domain_userid", "synth_browser_category", "synth_page_url", "synth_geo_city")


//Split the Tweets into two parts
val count_tweets = dfSynthEvents.count() // is equal to 3,505 + 8,893 = 12,398

val dfSynthTweetGen = dfTweets 
  .withColumn( "t_rn", row_number.over( Window.orderBy("t_id") ) ) // add a Row number so we can refer to this Tweet;
  .withColumn( "rand_nr_orderNr", lit(round(rand()*count_tweets))) 
  .withColumn( "t_browser_category", browser_tweet($"t_device") ) //Some extra data-cleaning
  .select("t_rn", "rand_nr_rowNumber", "t_unix_tstamp", "t_user_anonymized", "t_browser_category", "url", "user_location", "rand_nr_orderNr")

val dfSynthTweetsTrue = dfSynthTweetGen
  .orderBy(asc("rand_nr_orderNr")) 
  .limit(3506) //Take the first 3505 records of dfSynthTweetGen that are randomly ordered (3505 = 0.3*dfSynthEvents.count()).
  .withColumn( "rand_nr_events", lit(round(rand()*count_events))) 
  .join(dfSynthEvents, $"rand_nr_events" === dfSynthEvents.col("rn") ) //Get the values from a randomly selected dfSynthEvents record.
  .withColumn("true_match", lit(true)) //Assign a "true match" status
  .drop("rn");

val dfSynthTweetsFalse = dfSynthTweetGen
  .orderBy(desc("rand_nr_orderNr")) 
  .limit(8893) //take the bottom 8893 records
  .withColumn("true_match", lit(false)) //Assign a "false match" status
  .withColumn( "rand_nr_url_and_tstamp", lit(round(rand()*count_events)) ) //These identifiers have a dependency, so they must be sampled together.
  .withColumn( "rand_nr_location", lit(round(rand()*count_events)) ) 
  .withColumn( "rand_nr_browser", lit(round(rand()*count_events)) ) 
  .withColumn( "rand_nr_userid", lit(round(rand()*count_events)) ) 
  .select("true_match", "rand_nr_url_and_tstamp", "rand_nr_location", "rand_nr_browser", "rand_nr_userid");


/*  This block creates a frequency table for the intent times. We need to know how the intent time is distributed in the real datasets. */
val min_threshold = 0;
val max_threshold = 1200;

val dfRecordPairs = spark.read.parquet("s3://persgroep/ad/blocked_record_pairs"); //Gather all blocked record pairs
val dfPossibleMatches = dfRecordPairs
  .filter($"t_intent_time" >= min_threshold && $"t_intent_time" <= max_threshold) //Filter those that have a reasonable intent time.
  .withColumn("r_intent_time", ranges_of_5_seconds($"t_intent_time")); //Create a ranged intent time

val dfPossibleMatches = dfRecordPairs
  .filter($"t_intent_time" >= min_threshold && $"t_intent_time" <= max_threshold) //Filter those that have a reasonable intent time.
  .withColumn("ranged_intent_time", ranges_of_5_seconds($"t_intent_time")); //Create the ranged intent time

/* We know that there are true non-matches in our dataset that can have an intent time below 0. We also know that true matches never have an intent time below 0. */
val countNonMatches = dfRecordPairs
  .filter($"t_intent_time" >= -605 && $"t_intent_time" <= -10) //Filter those that have an unreasonable intent time.
  .withColumn("r_intent_time", ranges_of_5_seconds($"t_intent_time"))//Create a ranged intent time of 5 seconds
  .count();
val avg_false_matches = countNonMatches/120; //divide the count by the number of ranges (120)

/* End of block */

//Now we can use avg_false_matches to get an estimated of number of true matches per postitive timestep. 
//Group the (positive) intent times, so we get can count how often the ranges occur
val dfIntentTimesGrouped = dfPossibleMatches
  .groupBy("ranged_intent_time")
  .agg( count($"ranged_intent_time") as "count_ranged_intent_time" )
  .orderBy("ranged_intent_time") 
  .withColumn("reduced_count", remove_false_matches($"count_ranged_intent_time", lit(avg_false_matches)) ) //Reduce by avg_false_matches
  .withColumn("cumulative_count", sum($"reduced_count").over(Window.orderBy(asc("ranged_intent_time"))) ) //Create the cumulative count
  .withColumn("prev_cumulative_count", lag($"cumulative_count", 1, 0).over(Window.orderBy(asc("ranged_intent_time"))) ) //Get the cumulative count of the previous record


//Get the final value of cumulative count
val count_possibleMatches = dfPossibleMatches.count()

//Create the Tweets that will have a true match with a record in dfSynthEvents.
val dfSynthTweetsTrueNew = dfSynthTweetsTrue
  .drop("t_browser_category", "url", "user_location", "t_user_anonymized", "t_unix_tstamp", "rand_nr_rowNumber", "rand_nr_orderNr")
  .withColumnRenamed("unix_tstamp", "t_unix_tstamp")
  .withColumnRenamed("synth_domain_userid", "t_synth_domain_userid")
  .withColumnRenamed("synth_browser_category", "t_synth_browser_category")
  .withColumnRenamed("synth_page_url", "t_synth_page_url")
  .withColumn("rand_nr_cityChanger", lit(round(rand()*200))) //Generate a random number between 1 and 200.
  .withColumn("t_synth_location", //When that random number is less than 161 (80.5% chance), then change the city name changes beyond recognition.
     when( $"rand_nr_cityChanger" <= 161, lit("Nederland") ).otherwise($"synth_geo_city") 
  )
  .withColumn("rand_nr_intent_time", lit(round(rand()*count_possibleMatches))) //Generate random number between 1 and final_cumulative_count
  .join(dfIntentTimesGrouped, //Join based on where the random number lands. This returns an intent time between 0 and 600, in time-steps of 5 seconds.
     $"rand_nr_intent_time" <= dfIntentTimesGrouped.col("cumulative_count")
     && $"rand_nr_intent_time" > dfIntentTimesGrouped.col("prev_cumulative_count") 
  )
  .withColumn( "t_unix_tstamp", addition($"t_unix_tstamp", $"r_intent_time") ) //Add the intent time to the unix timestamp
  .orderBy("t_rn")
  .select("t_unix_tstamp", "t_synth_domain_userid", "t_synth_browser_category", "t_synth_page_url", "true_match", "t_synth_location", "r_intent_time")


//This calculation is for tstamp. This causes each time-step between 0 and 1200 to have, on average, the same amount of false matches.
val falsePostsCount = dfSynthTweetsFalse.count(); //Get the number of records in dfTweets that were assigned are selected as non-mathces.
val avg_false_matches_per_second = avg_false_matches.toFloat/5; //Divide by 5, because we had time-steps of 5 seconds.
val options_for_random = floor(falsePostsCount/avg_false_matches_per_second); //divide by the avg_false_matches_per_second.

val dfSynthTweetsFalseNew = dfSynthTweetsFalse
  .join(dfSynthTweetGen.select("t_rn", "t_user_anonymized"), $"rand_nr_userid" === dfSynthTweetGen.col("t_rn")) //Get a randomly chosen Twitter userID (anonymized)
  .drop("t_rn", "rand_nr_userid")
  .join(dfSynthTweetGen.select("t_rn", "t_browser_category"), $"rand_nr_browser" === dfSynthTweetGen.col("t_rn")) //Get a randomly chosen browser type from dfTweets
  .drop("t_rn", "rand_nr_browser")
  .join(dfSynthTweetGen.select("t_rn", "user_location"), $"rand_nr_location" === dfSynthTweetGen.col("t_rn")) //Get a randomly chosen user_location from dfTweets
  .drop("rand_nr_location")
  .join(dfSynthEvents.select("rn", "synth_page_url", "unix_tstamp"), $"rand_nr_url_and_tstamp" === dfSynthEvents.col("rn")) //Grab a random URL and timestamp from dfSynthEvents
  .orderBy("t_rn")
  .drop("t_rn", "rn", "rand_nr_url_and_tstamp")
  .withColumnRenamed("t_user_anonymized", "t_synth_domain_userid")
  .withColumnRenamed("t_browser_category", "t_synth_browser_category")
  .withColumnRenamed("user_location", "t_synth_location")
  .withColumnRenamed("synth_page_url", "t_synth_page_url") 
  .withColumn( "r_intent_time", lit(round(rand()*options_for_random)) ) //Create a random intent time.
  .withColumn("t_unix_tstamp", addition($"unix_tstamp", $"r_intent_time") ) //Add the intent time to the copied timestamp from dfSynthEvents. 
  .select("t_unix_tstamp", "t_synth_domain_userid", "t_synth_browser_category", "t_synth_page_url", "true_match", "t_synth_location", "r_intent_time");


val dfSynthTweets = dfSynthTweetsTrueNew.union(dfSynthTweetsFalseNew);


dfSynthTweets
  .coalesce(1) //Set number of partitions to 1
  .write
  .mode("overwrite")
  .parquet( "s3://persgroep/ad/generated_tweets/generated_tweets_x1.csv" ); //Write to S3
dfSynthEvents
  .coalesce(1)
  .write
  .mode("overwrite")
  .parquet( "s3://persgroep/ad/generated_events/generated_events_x1.csv" ); 
