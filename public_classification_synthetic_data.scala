// Databricks notebook source
import spark.implicits._
import sqlContext.implicits._

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

import collection.JavaConversions._
import java.time.{ZonedDateTime, ZoneId, LocalDate, LocalTime}

import com.github.mrpowers.spark.stringmetric.SimilarityFunctions._ //Jaro-Winkler comparison


//Data-cleaning, pre-processing and comparison functions.

//Clean URL: remove stuff like: ?session=x1%20id=123
val clean_url = udf {(url: String) => 
  if (url.contains("?")) url.slice( 0, url.indexOf("?") )
  else url
}

//Mathmatical functions
val time_diff = udf {(event1: Long, event2: Long) => 
  event2 - event1
}
val multiply = udf { ( value: Double, multiplier: Double  ) =>
  value * multiplier
}

//Comparisons
val temporal_score_function = udf { (
  distance: Double, 
  scale: Double, 
  offset: Double, 
  origin: Double,
  min_thresh: Double,
  max_thresh: Double
) =>
  if (distance>min_thresh && distance<max_thresh) { //Check for thresholds
	if (d_norm <= offset) 1 //If distance is lower than offset, award full score
	else BigDecimal(Math.pow(2.0, - (d_norm-offset) / scale)).setScale(2, BigDecimal.RoundingMode.HALF_EVEN).toDouble //Else award partial score
  } else { 0 }
}
val similarity_function = udf { ( string1: String, string2: String ) =>
  if (string2.contains( string1 )) 1
  else 0
}
val device_score_function = udf { ( event_device_cat: Int, post_device_cat: Int ) =>
  if (event_device_cat == post_device_cat) 1
  else 0
}

//Sum the scores
val sum_identifier_scores = udf { ( score_intent_time: Double, score_location: Int, score_device: Int  ) =>
  score_intent_time + score_location + score_device
}

//Probabilistic score weights (see notes in the bottom for explanation of the weights)
val prob_weigh_time = udf { (intent_time: Long) =>
  if (intent_time>0 && intent_time < 120) 6.2
  else -1.8
}
val prob_weigh_device = udf { (device_score: Int) =>
  if (device_score == 1) 2.3
  else -5.6
}
val prob_weigh_location = udf { (location_score: Int) =>
  if (location_score == 1) 6.2
  else -1.8
}
val prob_sum_scores = udf { ( score_intent_time: Double, score_location: Double, score_device: Double  ) =>
  score_intent_time + score_location + score_device
}


val dfEvents = spark.read.format("csv")
  .option("header", "true")
  .load("s3://persgroep/ad/generated_tweets/generated_tweets_x1.csv")
  .orderBy($"unix_tstamp")
  .cache();
  
val dfTweets = spark.read.format("csv")
  .option("header", "true")
  .load("s3://persgroep/ad/generated_tweets/generated_tweets_x1.csv")
  .orderBy($"t_unix_tstamp")
  .cache();


//Blocking phase
val dfRecordPairs = dfEvents
  .join( dfTweets, dfEvents.col("synth_page_url") === dfTweets.col("t_synth_page_url") ) //Inner join on URL (drops all events with no corresponding Tweets)
  .withColumn( "t_intent_time", time_diff($"unix_tstamp",  $"t_unix_tstamp") ) //Create the intent time col
  .cache()


//Comparison and classification phase
val dfMatchScores = dfRecordPairs
  //comparison
  .withColumn( "sim_intent_time", temporal_score_function($"t_intent_time", lit(110.0), lit(5.0), lit(5.0), lit(0.0), lit(300.0) ) ) //Calculate the temporal distance
  .withColumn( "sim_intent_time_tripled", multiply($"sim_intent_time", lit(3.0) ) ) //Triple the weight
  .withColumn(
    "score_location",
    when(
      col("synth_geo_city").isNotNull,
      similarity_function(lower($"synth_geo_city"), lower($"t_synth_location")) //If not Null, calculate identifier score
    ).otherwise( lit(0) ) //Otherwise score is zero.
  )
  .withColumn( "score_device", device_score_function($"synth_browser_category", $"t_synth_browser_category") )
  //classification cost-based
  .withColumn( "match_score", sum_identifier_scores($"sim_intent_time_tripled", $"score_location", $"score_device")  )
  //classification probabilistic
  .withColumn( "prob_score_device", prob_weigh_device($"score_device")  )
  .withColumn( "prob_score_location", prob_weigh_location($"score_location")  )
  .withColumn( "prob_score_time", prob_weigh_time($"t_intent_time")  )
  .withColumn( "prob_score_summed", prob_sum_scores($"prob_score_time", $"prob_score_location", $"prob_score_device") )


//Cost-based validation
val considered_pairs = dfMatchScores.count();
val threshold = 3.00
val true_matches = dfMatchScores.filter($"true_match" === true && $"r_intent_time" === $"t_intent_time" ).count();
val true_matches_found = dfMatchScores.filter($"true_match" === true && $"r_intent_time" === $"t_intent_time" && $"match_score" >= threshold ).count();
val true_matches_not_found = true_matches - true_matches_found;

val false_matches_pt1 = dfMatchScores.filter($"true_match" === true && $"r_intent_time" =!= $"t_intent_time" && $"match_score" >= threshold ).count();
val false_matches_pt2 = dfMatchScores.filter($"true_match" === false && $"match_score" >= threshold ).count();
val false_matches = false_matches_pt1 + false_matches_pt2;

val true_nonmatches_pt1 = dfMatchScores.filter($"true_match" === true && $"r_intent_time" =!= $"t_intent_time" && $"match_score" < threshold ).count();
val true_nonmatches_pt2 = dfMatchScores.filter($"true_match" === false && $"match_score" < threshold ).count();
val true_nonmatches = true_nonmatches_pt1 + true_nonmatches_pt2;

val recall: Float = true_matches_found.toFloat/true_matches;
val precision: Float = true_matches_found.toFloat/(true_matches_found+false_matches);
val f_score = 2 * (recall * precision)/(recall + precision);

println("recall: " + recall)
println("precision: " + precision)
println("f1-score: " + f_score)


//Probablistic validation
val considered_pairs = dfMatchScores.count();
val threshold = 9.03 //See notes in the bottom for explanation of this number

val true_matches = dfMatchScores.filter($"true_match" === true && $"r_intent_time" === $"t_intent_time" ).count();
val true_matches_found = dfMatchScores.filter($"true_match" === true && $"r_intent_time" === $"t_intent_time" && $"prob_score_summed" >= threshold ).count();
val true_matches_not_found = true_matches - true_matches_found;

val false_matches_pt1 = dfMatchScores.filter($"true_match" === true && $"r_intent_time" =!= $"t_intent_time" && $"prob_score_summed" >= threshold ).count();
val false_matches_pt2 = dfMatchScores.filter($"true_match" === false && $"prob_score_summed" >= threshold ).count();
val false_matches = false_matches_pt1 + false_matches_pt2;

val true_nonmatches_pt1 = dfMatchScores.filter($"true_match" === true && $"r_intent_time" =!= $"t_intent_time" && $"prob_score_summed" < threshold ).count();
val true_nonmatches_pt2 = dfMatchScores.filter($"true_match" === false && $"prob_score_summed" < threshold ).count();
val true_nonmatches = true_nonmatches_pt1 + true_nonmatches_pt2;

val recall: Float = true_matches_found.toFloat/true_matches;
val precision: Float = true_matches_found.toFloat/(true_matches_found+false_matches);
val f_score = 2 * (recall * precision)/(recall + precision);

println("recall: " + recall)
println("precision: " + precision)
println("f1-score: " + f_score)


// End of Code 


/* Probabilistic weight notes. See section 2: Related work of Erik's Thesis for an explanation of the formulas.

true matches = 3505
(a*b) = 211050 considered record pairs after blocking 
starting weight = log2(true matches/(a*b)-true matches)
starting weight = log2(3505/(211050-3505))
starting weight = -5.88

P = 0.9
threshold weight = log2(P/1-P)
threshold wieght = log2(0.9/1-0.9)
threshold wieght = 3.15

threshold = starting weight - threshold weight
threshold = -5.88 - 3.15
threshold = 9.03


Weights per identifier:

i3 temporal
M = 0.721
U = 0.010
Wm = 6.17
Wu = -1.78

i4 location
M = 0.2
U = 0.0175
Wm = 3.55
Wu = -0.30

i5 device type
M = 0.981
U = 0.203
Wm = 2.27
Wu = -5.64
*/
