import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

/**
 * @author anewman@us.ibm and redman@us.ibm
 */
object TweetEat extends App {

  def getAlchemyResults( tweet:String, call:String ): String = {
    //insert apikey here
    val apikey = ""

    val alchemyurl = "http://access.alchemyapi.com/calls/text/"
    val baseurl = alchemyurl + call
    val tweetParsed = tweet.replaceAll("[^0-9a-zA-Z_\\s]", "").replaceAll(" ","%20");
    val tweetEncoded = URLEncoder.encode( tweet, "UTF-8" )
    val url2 = baseurl +"?apikey=" + apikey + "&text=" + tweetEncoded + "&showSourceText=1";
    val result2 = scala.io.Source.fromURL(url2).mkString

    return result2

  }


  //xml parser
  def parseAlchemyResponseEntity(alchemyResponse:String,typeResponse:String) : Array[String] = {

      var entitiesText = ""
      if ((alchemyResponse contains "<entities>") && (alchemyResponse contains "<entity>")){
        val entitiesBegin = alchemyResponse indexOf "<entities>"
        val entitiesEnd = alchemyResponse indexOf "</entities>"
        entitiesText = alchemyResponse.substring(entitiesBegin + "<entities>".length,entitiesEnd)
      }
      else
      {
        return new Array[String](0)
      }

      val numEntities = "<entity>".r.findAllIn(entitiesText).length
      var entityTextArray = new Array[String](numEntities)

      var i = 0
      for (i <- 0 to numEntities-1)
      {
        println(i)
        println(entitiesText)
        val tempEntityText = parseAlchemyResponseIndividual(entitiesText,typeResponse)
        entityTextArray(i) = tempEntityText
        val endOfCurrentEntity = entitiesText indexOf "</entity>"
        entitiesText = entitiesText.substring(endOfCurrentEntity+"</entity>".length)
      }

      return entityTextArray
    }

   def parseAlchemyResponseIndividual(alchemyResponse:String,typeResponse:String) : String = {

     if(alchemyResponse contains "<" + typeResponse + ">"){
       val scoreBegin = alchemyResponse indexOf "<" + typeResponse + ">"
       val scoreEnd = alchemyResponse indexOf "</" + typeResponse + ">"
       val isolatedString = alchemyResponse.substring(scoreBegin + typeResponse.length+2,scoreEnd)
       return isolatedString
     }
     else
       return "None"
   }

    var entityMap = collection.mutable.Map[String, collection.mutable.Map[String,Double]]()
    //handle aggregation
    //{ <entity> : { numObservations: <Double>, scoreTotal: <Double>, average:<Double>} }
    def aggregateWrapper(entityArray:Array[String],sentimentScore:String) =
    {
      var entityString = ""
      for (entityString <- entityArray)
      {
        aggregateSingleEntityAndSentiments(entityString,sentimentScore)
      }
    }

    def aggregateSingleEntityAndSentiments(entity:String,sentimentScore:String) =
    {
      var newMap = collection.mutable.Map[String,Double]()

      //never seen the entity
      if (!(entityMap contains entity))
      {
        newMap.put("numObservations", 1)
        newMap.put("scoreTotal",sentimentScore.toDouble)
        newMap.put("average",sentimentScore.toDouble)
      }
      //seen the entity before
      else
      {
        var oldMap = entityMap.get(entity)
        val newNumObservations = oldMap.get("numObservations")+1.0
        val newScoreTotal = oldMap.get("scoreTotal")+sentimentScore.toDouble
        newMap.put("numObservations",newNumObservations)
        newMap.put("scoreTotal",newScoreTotal)
        newMap.put("average",newScoreTotal / newNumObservations)
      }
      entityMap.put(entity, newMap)
    }

  val batchInterval_s = 1
  val totalRuntime_s = 32

  val propPrefix = "twitter4j.oauth."
  System.setProperty(s"${propPrefix}consumerKey", "")
  System.setProperty(s"${propPrefix}consumerSecret", "")
  System.setProperty(s"${propPrefix}accessToken", "")
  System.setProperty(s"${propPrefix}accessTokenSecret", "")

  // create SparkConf
  val conf = new SparkConf().setAppName("mids tweeteat")

  // batch interval determines how often Spark creates an RDD out of incoming data
  val ssc = new StreamingContext(conf, Seconds(batchInterval_s))
  val stream = TwitterUtils.createStream(ssc, None)

  // extract desired data from each status during sample period as class "TweetData", store collection of those in new RDD
  //val tweetData = stream.map(status => TweetData(status.getId, status.getUser.getScreenName, status.getText.trim))

  val rawTweets = stream.map(status => status.getText())

  rawTweets.foreachRDD(rdd => {
    // data aggregated in the driver

    println(" ")


    for(tweet <- rdd.collect().toArray) {

        val alchemyResponseSentiment = getAlchemyResults(tweet, "TextGetTextSentiment" )
        val alchemyResponseEntity = getAlchemyResults(tweet, "TextGetRankedNamedEntities" )
        println( " " )
        println(tweet);
        println( " " )
        println( alchemyResponseSentiment )
        println( alchemyResponseEntity )
        println( " " )

        val sentimentScore = parseAlchemyResponseIndividual(alchemyResponseSentiment,"score")
        val sentimentType = parseAlchemyResponseIndividual(alchemyResponseSentiment,"type")
        val language = parseAlchemyResponseIndividual(alchemyResponseSentiment,"language")
        val entities = parseAlchemyResponseEntity(alchemyResponseEntity,"text")

        println("sentiment type: " + sentimentType)
        println("sentiment score: " + sentimentScore)
        println("language: " + language)
        println("entities: " + entities.mkString(", "))

        aggregateWrapper(entities,sentimentScore)

        entityMap.keys.foreach{ i =>
                     print( "Key = " + i )
                     println(" Value = " + entityMap(i) )}
    }

    println(" ")

  })


  // start consuming stream
  ssc.start
  ssc.awaitTerminationOrTimeout(totalRuntime_s * 1000)
  ssc.stop(true, true)

  println(s"============ Exiting ================")
  System.exit(0)
}



