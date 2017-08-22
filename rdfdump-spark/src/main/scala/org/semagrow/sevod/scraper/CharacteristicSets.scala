package org.semagrow.sevod.scraper

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.semagrow.sevod.scraper.io.TriplesIOOps._
/**
  * Created by angel on 27/7/2016.
  */

object CharacteristicSets {

  import org.semagrow.sevod.model.TriplifierImplicits._


  def main(args : Array[String]) : Unit = {

    val sparkConfig = new SparkConf()
      .setAppName("Characteristic Sets")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.semagrow.sevod.scraper.io.TriplesIOOps$JenaKryoRegistrator")

    val sc = new SparkContext(sparkConfig)

    if (args.length < 1)
      System.err.println("No path is defined")
    else {
      val path = args(0)

      val predicatesOfSubject = sc.nTriplesFile(path)
        .map(t => (t.getSubject, t.getPredicate))
        .partitionBy(new HashPartitioner(16))
        .groupByKey()
        .map {
          case (s,p) => (s, p.toSeq.distinct)
        }

      predicatesOfSubject.map {
        case (s, p_set) => (p_set, 1)
      }.coalesce(4)
       .reduceByKey(_+_)
       .saveAsTextFile(args(1))
    }

  }

}