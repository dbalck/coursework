/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import scala.Tuple3;
import twitter4j.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Comparator;
import java.io.Serializable;


/**
 * Displays the most positive hash tags by joining the streaming Twitter data with a static RDD of
 * the AFINN word list (http://neuro.imm.dtu.dk/wiki/AFINN)
 */
public class PopHash {

  public static void main(String[] args) {
    if (args.length < 5) {
      System.err.println("Usage: <consumer key>" +
        " <consumer secret> <access token> <access token secret> [<filters>]");
      System.exit(1);
    }


    String consumerKey = args[0];
    String consumerSecret = args[1];
    String accessToken = args[2];
    String accessTokenSecret = args[3];
    Integer topHashCountPerWindow = new Integer(args[4]);
    System.out.println("Starting!!");
    String[] filters = Arrays.copyOfRange(args, 5, args.length);

    // Set the system properties so that Twitter4j library used by Twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
    System.setProperty("twitter4j.oauth.accessToken", accessToken);
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);


    SparkConf sparkConf = new SparkConf().setAppName("PopHash");

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]");
      System.out.println("running local mode, spark.master not found");
    }

    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);

    // map the tweet to its text as a k-v
    JavaPairDStream<Status, String> statusToText = stream
      .mapToPair(s -> new Tuple2<Status, String>(s, s.getText()));

    // map the #hashtag words in the value text to the tweet
    JavaPairDStream<Status, String> statusToWord = statusToText
      .flatMapValues(text -> Arrays.asList(text.split(" ")));

    // status to hashtag
    // filter for the #hashtag words in the value text to the tweet
    JavaPairDStream<Status, String> statusToHashTag = statusToWord
      .filter(pair -> pair._2().startsWith("#")); 

    // filter the @user words in the value text to the tweet
    JavaPairDStream<Status, String> statusToUser = statusToWord
      .filter(pair -> pair._2().startsWith("@")); 

    // count the frequencies of the hashtags
    JavaPairDStream<String, Integer> hashTagToCount = statusToHashTag
      .mapToPair(pair -> new Tuple2<String, Integer>(pair._2(), 1))
      .reduceByKeyAndWindow((x,y) -> x + y, new Duration(10000));


    // swap the status message with the hashtags
    JavaPairDStream<String, Status> hashTagToStatus = statusToHashTag
      .mapToPair(x -> x.swap());

    // hashtag to user
    JavaPairDStream<String, String> hashTagToUser = statusToHashTag 
      .mapToPair(x -> x.swap())
      .mapToPair(pair -> new Tuple2<String, String>(pair._1(), pair._2().getText()))
      .flatMapValues(text -> Arrays.asList(text.split(" ")))
      .filter(pair -> pair._2().startsWith("@"))
      .reduceByKeyAndWindow((x,y) -> x , new Duration(10000));


    JavaPairDStream<Tuple2<Integer, String>, String> countAndUserToHashTag = hashTagToCount
      .join(hashTagToUser)
      .mapToPair(x -> x.swap());


    // sort the tuples from most frequent to least
    List<Tuple2<Integer, String>, String> countToHashTagAndUserSorted = countAndUserToHashTag 
      .transformToPair(rdd -> rdd
        .sortByKey(new TwitterComparator(), false)
        .top(topHashCountPerWindow));

    //countToHashTagAndUserSorted.print();
    System.out.print(countToHashTagAndUserSorted);


    // JavaPairDStream<String, Integer> hashTagMentions = statusToHashTags
    //   .mapToPair(hash -> new Tuple2<String, Integer>(hash, 1))
    //   .reduceByKeyAndWindow((x,y) -> x + y, new Duration(5000));



    jssc.start();
    try {
      jssc.awaitTerminationOrTimeout(31000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

class TwitterComparator implements Serializable, Comparator<Tuple2<Integer, String>> {

    public int compare(Tuple2<Integer, String> a, Tuple2<Integer, String> b) {
      if (a._1() < b._1()) {
        return -1;
      } else if (a._1() > b._1()) {
        return 1;
      } else {
        return 0;
      }
    }
}
