import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.json4s._
import org.json4s.jackson.JsonMethods._

import Mozilla.Telemetry._

object Analysis{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mozilla-telemetry").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)
    implicit lazy val formats = DefaultFormats

    val pings = Pings("Firefox", "nightly", "36.0a1", "*", ("20141109", "20141110")).RDD(0.1)

    var osdistribution = pings.map(line => {
      ((parse(line.substring(37)) \ "info" \ "OS").extract[String], 1)
    }).reduceByKey(_+_).collect

    println("OS distribution:")
    osdistribution.map(println)

    sc.stop()
  }
}
