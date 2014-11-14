import scala.io.Source

import java.io.BufferedInputStream

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.commons.compress.compressors._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL.WithDouble._

import dispatch._, Defaults._

import awscala._, s3._

object MozillaTelemetry{
  implicit lazy val s3 = S3()
  implicit lazy val formats = DefaultFormats

  val filter =
    """
     { "filter":
       {
         "version": 1,
         "dimensions": [
           {
             "field_name": "reason",
             "allowed_values": ["saved-session"]
           },
           {
             "field_name": "appName",
             "allowed_values": ["Fennec"]
           },
           {
             "field_name": "appUpdateChannel",
             "allowed_values": ["nightly"]
           },
           {
             "field_name": "appVersion",
             "allowed_values": ["36.0a1"]
           },
           {
             "field_name": "appBuildID",
             "allowed_values": ["20141110030204"]
           },
           {
             "field_name": "submission_date",
             "allowed_values": ["20141110"]
           }
         ]
       }
     }
     """

  def getS3Filenames(filter: String) = {
    val svc = url("http://ec2-54-203-209-235.us-west-2.compute.amazonaws.com:8080/files").POST
      .setBody(filter)
      .addHeader("Content-type", "application/json")

    val req = Http(svc)
    val JArray(jsonList) = parse(req().getResponseBody) \ "files"
    jsonList.map(_.extract[String])
  }

  def readS3File(filename: String) = {
    val bucket = s3.bucket("telemetry-published-v2").get
    val obj = s3.get(bucket, filename).get
    val stream = new BufferedInputStream(obj.getObjectContent)
    val compressedStream = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.LZMA, stream)
    val content = Source.fromInputStream(compressedStream).getLines.toArray
    compressedStream.close
    content
  }

  def startAnalysis() {
    val conf = new SparkConf().setAppName("mozilla-telemetry").setMaster("local[8]")
    val sc = new SparkContext(conf)

    val filenames = getS3Filenames(filter)
    val dataset = sc.parallelize(filenames).flatMap(filename => {
      readS3File(filename)
    }) // repartition?

    analysis(dataset)

    sc.stop()
  }

  def analysis(pings: RDD[String]) {
    ////////////////////////////////////////////////////////////////////////////
    // Your analysis code starts here
    ////////////////////////////////////////////////////////////////////////////

    var osdistribution = pings.map(line => {
      ((parse(line.substring(37)) \ "info" \ "OS").extract[String], 1)
    }).reduceByKey(_+_).collect

    println("OS distribution:")
    osdistribution.map(println)

    ////////////////////////////////////////////////////////////////////////////
    // Your analysis ends here
    ////////////////////////////////////////////////////////////////////////////
  }

  def main(args: Array[String]) {
    startAnalysis()
  }
}
