import scala.io.Source
import scala.math.max

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

package Mozilla {
  package Telemetry{
    object Pings {
      def apply(appName: String, channel: String, version: Tuple2[String, String], buildid: Tuple2[String, String], submission: Tuple2[String, String]) = {
        new Pings(appName, channel, version, buildid, submission)
      }

      def apply(appName: String, channel: String, version: String, buildid: String, submission: String) = {
        new Pings(appName, channel, (version, version), (buildid, buildid), (submission, submission))
      }

      def apply(appName: String, channel: String, version: String, buildid: String, submission: Tuple2[String, String]) = {
        new Pings(appName, channel, (version, version), (buildid, buildid), submission)
      }

      def apply(appName: String, channel: String, version: String, buildid: Tuple2[String, String], submission: String) = {
        new Pings(appName, channel, (version, version), buildid, (submission, submission))
      }

      def apply(appName: String, channel: String, version: String, buildid: Tuple2[String, String], submission: Tuple2[String, String]) = {
        new Pings(appName, channel, (version, version), buildid, submission)
      }
    }

    class Pings(appName: String, channel: String, version: Tuple2[String, String], buildid: Tuple2[String, String], submission: Tuple2[String, String]) extends Serializable{
      implicit lazy val s3 = S3()
      lazy val bucket = s3.bucket("telemetry-published-v2").get
      val filter = s"""
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
                "allowed_values": ["$appName"]
              },
              {
                "field_name": "appUpdateChannel",
                "allowed_values": ["$channel"]
              },
              {
                "field_name": "appVersion",
                "allowed_values": ${generateField(version)}
              },
              {
                "field_name": "appBuildID",
                "allowed_values": ${generateField(buildid)}
              },
              {
                "field_name": "submission_date",
                "allowed_values": ${generateField(submission)} 
              }
            ]
          }
        }"""

      def generateField(field: Tuple2[String, String]) = {
        if(field._1 == "*" && field._2 == "*") {
          """"*""""
        } else {
          s"""{"min": "${field._1}", "max": "${field._2}"}"""
        }
      }

      def filenames = {
        implicit val formats = DefaultFormats

        val svc = url("http://ec2-54-203-209-235.us-west-2.compute.amazonaws.com:8080/files").POST
          .setBody(filter)
          .addHeader("Content-type", "application/json")

        try {
          val req = Http(svc)
          val JArray(jsonList) = parse(req().getResponseBody) \ "files"

          jsonList.map(_.extract[String])
        } catch {
          case e: Exception => {
            println("Warning, no matches for filter")
            List[String]()
          }
        }
      }

      def read(filename: String) = {
        val obj = s3.get(bucket, filename).get
        val stream = new BufferedInputStream(obj.getObjectContent)
        val compressedStream = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.LZMA, stream)
        val source = Source.fromInputStream(compressedStream)
        val content = source.getLines.toArray

        source.close
        content
      }

      def RDD(fraction: Double = 1.0)(implicit sc: SparkContext) = {
        val sample = filenames.slice(0, filenames.length.toDouble * fraction toInt)
        val parallelism = max(sample.length / 16, sc.defaultParallelism)

        sc.parallelize(sample, parallelism).flatMap(filename => {
          read(filename)
        })
      }
    }
  }
}
