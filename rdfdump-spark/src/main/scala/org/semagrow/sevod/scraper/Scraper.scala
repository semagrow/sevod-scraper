package org.semagrow.sevod.scraper

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.semagrow.sevod.model.Triplifyable
import org.semagrow.sevod.scraper.Scraper.Stats
import org.semagrow.sevod.scraper.io.TriplesIOOps._
import org.apache.jena.graph._
import org.apache.jena.datatypes.xsd._

/**
  * Created by angel on 25/7/2016.
  */
object Scraper {

  import org.semagrow.sevod.model.TriplifierImplicits._

  def main(args : Array[String]) {

    val sparkConfig = new SparkConf()
        .setMaster("local[*]")
      .setAppName("SEVOD Stats")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.semagrow.sevod.scraper.io.TriplesIOOps$JenaKryoRegistrator")

    val sc = new SparkContext(sparkConfig)

    if (args.length < 1)
      System.err.println("No path is defined")
    else {
      val path = args(0)

      val triples = sc
        .nTriplesFile(path)
        .scrape()
        .saveAsNTriplesFile(args(1))
    }
    sc.stop()
  }

  case class Stats(property: Node,
                   count : Long,
                   distinctSubjects: Long,
                   distinctObjects: Long) extends Triplifyable[Stats] {

    val ns = "http://rdfs.org/ns/void#"

    override def triplify(): Seq[Triple] = {
      val n = NodeFactory.createBlankNode()
      Seq(
        t(n, u(ns,"property"), property),
        t(n, u(ns,"triples"), l(count)),
        t(n, u(ns,"distinctSubjects"), l(distinctSubjects)),
        t(n, u(ns,"distinctObjects"), l(distinctObjects))
      )
    }

    def t(s: Node, p : Node, o: Node) = new Triple(s,p,o)
    def u(s: String) = NodeFactory.createURI(s)
    def u(p:String, s: String) = NodeFactory.createURI(p.concat(s))
    def l(o : Long) = NodeFactory.createLiteralByValue(o, XSDDatatype.XSDinteger)

  }

  implicit def rddToScraper(triples : RDD[Triple]): Scraper = new Scraper(triples)

}

class Scraper (triples : RDD[Triple]) {

  def scrape() : RDD[Stats] = {

    val predicatePartitioner = new HashPartitioner(triples.context.defaultParallelism)

    val triplesByPred = triples
      .keyBy(t => t.getPredicate)
      .partitionBy(predicatePartitioner).persist()

    val count        = triplesByPred.mapValues(t => 1).reduceByKey(_+_)
    val subjectCount = triplesByPred.mapValues(_.getSubject).countApproxDistinctByKey()
    val objectCount  = triplesByPred.mapValues(_.getObject).countApproxDistinctByKey()

    count.join(subjectCount).join(objectCount)
      .coalesce(triples.context.defaultMinPartitions)
      .map {
        case (n,((c,s),o)) =>  Stats(n, c, s, o)
     }
  }

}



