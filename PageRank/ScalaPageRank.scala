import java.io.{File, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by Pawan Araballi on 11/5/2016.
  */
object ScalaPageRank {

  def main(args: Array[String]) {

    val patterntitle = new Regex("<title>(.*?)</title>")
    val patterntext = new Regex("<text+\\s*[^>]*>(.*?)</text>")
    val patternredirect = new Regex("\\[\\[(.*?)\\]\\]")

	//Initialize the sparksession
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir","file:///c:/tmp/spark-warehouse")
      .master("local")
      .appName("ScalaPageRank")
      .getOrCreate()

    val inputfeed = new java.lang.StringBuilder();
    val filename = "fileopen.scala"
    var nooftitles = 0;
    for (line <- scala.io.Source.fromFile(args(0).getLines()) {

      nooftitles = nooftitles + 1; // Counting the number of lines
	  
	  // Performing pattern matching with the title and text to get outlinks
      var pagetitle = (patterntitle findAllMatchIn line).mkString(",") replaceAll("</title>|<title>","")
      var texts = (patterntext findAllMatchIn line).mkString("&#&#&")
      var noofoutlinks = (patternredirect findAllMatchIn texts).size
      var outlinks = (patternredirect findAllMatchIn texts).mkString("&#&#&") replaceAll("\\[\\[|\\]\\]","")
	  
	  //If outlinks is more than 1 then append each 
      if(noofoutlinks > 1){
        outlinks.split("&#&#&").foreach{item => inputfeed.append(pagetitle + "#####" + item + "\n")}
      }else if (!outlinks.isEmpty){
        inputfeed.append(pagetitle + "#####" + outlinks + "\n")
      }
    }
    //println("Number of titles: ",nooftitles)
    var initpagerank :Double = 0.0

    //Calculate the initial page rank by 1/N where N is number of titles
    initpagerank = (1.0/nooftitles)

    //println("Initial Page Rank: ",initpagerank)

	//Writing the content to intermediate file
    val writer = new PrintWriter(new File("intermediatefile.txt"))
    writer.write(inputfeed.toString)
    writer.close()
	
	
    val iterations = if (args.length > 1) args(1).toInt else 10
    val lines = spark.read.textFile("intermediatefile.txt").rdd // Initialize the rdd 
    val links = lines.map { s =>
      val parts = s.split("#####")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => initpagerank)
    //for (line <- ranks) print("These are Ranks",line)
	
	// Helper variables to perform the page rank convergence 
	// The is to calculate the page rank of all the links and sum it and then compare the current sum with previous sum
    var prevsum : Double = 0
    var currsum : Double = 0
    var flag : Boolean = false
	
	// Number of iterations
    for (i <- 1 to iterations)
    {
      currsum = 0 //Every iteration the currsum must be initialized to zero
      
	  //Contributions of each node is calculted below
      val contribs = links.join(ranks).values.flatMap
      { case (urls, rank) =>
            val size = urls.size
            urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

      //ranks.collect()foreach (println)
	  
	  // Sum of all the page ranks are calculated here
      ranks.collect()foreach {
        keyVal => currsum = currsum + keyVal._2
      }

      //println(i, "iteration currsum: ", currsum)
      //println(i, "iteration prevsum: ", prevsum)
	  
	  // If the currsum is equal to prevsum then write the content i.e page with pagerank to output file
      if(currsum == prevsum){
        val output = ranks.collect()
		val finalres = new PrintWriter(new File("FinalResult.txt"))
        val output = ranks.collect().sortWith(_._2>_._2)
		output.foreach(tup => finalres.write(tup._1 + " has rank: " + tup._2 + "." + "\n"))
		finalres.close()
        flag = true
        FileUtils.deleteQuietly(new File("Tempoutput1.txt"))
        spark.stop()
        System.exit(0)
      }
        prevsum = currsum
    }

    if(flag != true){
      val output = ranks.collect()
	  val finalres = new PrintWriter(new File("FinalResult.txt"))
	  val output = ranks.collect().sortWith(_._2>_._2)
      output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
      FileUtils.deleteQuietly(new File("Tempoutput1.txt"))
      spark.stop()
    }
  }
}