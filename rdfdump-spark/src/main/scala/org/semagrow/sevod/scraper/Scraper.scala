package org.semagrow.sevod.scraper

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.semagrow.sevod.scraper.io.TriplesIOOps._
import org.apache.jena.graph._

import scala.reflect.ClassTag


/**
  * Created by angel on 25/7/2016.
  */
object Scraper {

  import org.semagrow.sevod.model.TriplifierImplicits._

  val subjectTrieParameterDefault = "15"
  val objectTrieParameterDefault = "150"

  val usage = "USAGE:" +
    "\n\t scala " + Scraper.getClass + " [input] [endpoint_url] [output]" +
    "\n\t scala " + Scraper.getClass + " [input] [endpoint_url] [subjectBound] [objectBound] [output]"

  def main(args : Array[String]) {

    if (args.length != 5 && args.length != 3) {
      throw new IllegalArgumentException(usage)
    }
    else {

      val path = args(0)
      val endpoint = args(1)
      val flags = "-spov"

      val subjectTrieParameter = if (args.length == 3) subjectTrieParameterDefault else args(2)
      val objectTrieParameter =  if (args.length == 3) objectTrieParameterDefault  else args(3)

      val output = args(args.length-1)

      val datasetId = System.currentTimeMillis.toString

      val sparkConfig = new SparkConf()
        //.setMaster("local[*]")
        .setAppName("SEVOD Stats")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "org.semagrow.sevod.scraper.io.TriplesIOOps$JenaKryoRegistrator")
        .set("sparqlEndpoint", endpoint)
        .set("datasetId", datasetId)
        .set("subjectTrieParameter", subjectTrieParameter)
        .set("objectTrieParameter", objectTrieParameter)

      val sc = new SparkContext(sparkConfig)

      val triples = sc
        .inputFile(path)
        .scrape(flags)
        .saveAsNTriplesFile(output)

      sc.stop()
    }
  }

  implicit def rddToScraper(triples : RDD[Triple]): Scraper = new Scraper(triples)

  def getVocabulary(node : Node) : Node =
    NodeFactory.createURI(node.getURI.substring(0, node.getURI.indexOf('/', "http://".size + 1) + 1))
}

class Scraper (triples : RDD[Triple]) {

  import Statistics._
  import Utils._

  def countTriples[T: ClassTag](trdd: RDD[(T,Triple)]) =
    trdd
      .mapValues(t => 1).reduceByKey(_+_)

  def countDistSubjects[T: ClassTag](trdd: RDD[(T,Triple)]) =
    trdd
      .mapValues(_.getSubject).distinct()
      .mapValues(t => 1).reduceByKey(_+_)

  def countDistObjects[T: ClassTag](trdd: RDD[(T,Triple)]) =
    trdd.mapValues(_.getObject).distinct()
      .mapValues(t => 1).reduceByKey(_+_)

  /* scrape Predicates (simple and with vocabularies) */

  def scrapePredicates() : RDD[Stats] = {

    val predicatePartitioner = new HashPartitioner(triples.context.defaultParallelism)

    val triplesByPred = triples
      .keyBy(t => t.getPredicate)
      .partitionBy(predicatePartitioner).persist()

    val count        = countTriples(triplesByPred)
    val subjectCount = countDistSubjects(triplesByPred)
    val objectCount  = countDistObjects(triplesByPred)

    val datasetId = triples.context.getConf.get("datasetId")

    count.join(subjectCount).join(objectCount)
      .coalesce(triples.context.defaultMinPartitions)
      .map {
        case (n,((c,s),o)) => VoidStats(datasetId, n, c, s, o)
     }
  }

  def scrapePredicatesVoc() : RDD[Stats] = {

    val predicatePartitioner = new HashPartitioner(triples.context.defaultParallelism)

    val triplesByPred = triples
      .keyBy(t => t.getPredicate)
      .partitionBy(predicatePartitioner).persist()

    val count        = countTriples(triplesByPred)
    val subjectCount = countDistSubjects(triplesByPred)
    val objectCount  = countDistObjects(triplesByPred)

    def vocabulariesOf(elems : RDD[(Node, Node)]) : RDD[(Node, Iterable[Node])] =
      elems
        .filter(_._2.isURI).filter(_._2.getURI.startsWith("http://"))
        .mapValues(Scraper.getVocabulary(_))
        .distinct().groupByKey()

    val subjVocab = vocabulariesOf(triplesByPred.mapValues(_.getSubject))
    val objVocab  = vocabulariesOf(triplesByPred.mapValues(_.getObject))

    val datasetId = triples.context.getConf.get("datasetId")

    count.join(subjectCount).join(objectCount).leftOuterJoin(subjVocab).leftOuterJoin(objVocab)
      .coalesce(triples.context.defaultMinPartitions)
      .map {
        case (n,((((c,s),o),sv),ov)) => PredStats(VoidStats(datasetId, n, c, s, o), sv, ov)
      }
  }

  /* scrape Subjects and Objects */

  def scrapeUris(f: Triple => Node, trieParameter: Integer, label: String) : RDD[Stats] = {

    val prefixPartitioner = new HashPartitioner(triples.context.defaultParallelism)

    val uris = triples.map(f(_)).filter(_.isURI).map(_.getURI)
    val prefixes = PathTrie.getPrefixes(uris, trieParameter)

    var prefixMap = uris.context.broadcast(prefixes.collect())

    val urisByPrefix = triples.filter(_.getObject.isURI)
      .flatMap(t => prefixMap.value.filter(p => t.getObject.getURI.startsWith(p)).map(p => (t, p)))
      .keyBy(_._2).mapValues(_._1)
      .partitionBy(prefixPartitioner).persist()

    val count        = countTriples(urisByPrefix)
    val subjectCount = countDistSubjects(urisByPrefix)
    val objectCount  = countDistObjects(urisByPrefix)

    val datasetId = triples.context.getConf.get("datasetId")

    count.join(subjectCount).join(objectCount)
      .coalesce(triples.context.defaultMinPartitions)
      .map {
        case (n, ((c,s),o)) => PrefixStats(datasetId, label, n, c, s, o)
      }
  }

  def scrapeSubjects() : RDD[Stats] = {
    val subjectTrieParameter = Integer.valueOf(triples.context.getConf.get("subjectTrieParameter"))
    scrapeUris(_.getSubject, subjectTrieParameter, "subject")
  }

  def scrapeObjects() : RDD[Stats] = {
    val objectTrieParameter = Integer.valueOf(triples.context.getConf.get("objectTrieParameter"))
    scrapeUris(_.getObject, objectTrieParameter, "object")
  }

  /* scrape Classes */

  def scrapeClasses() : RDD[Stats] = {

    val classPartitioner = new HashPartitioner(triples.context.defaultParallelism)

    val triplesByClass = triples
      .filter(_.getPredicate.equals(u(rdf, "type")))
      .keyBy(t => t.getObject)
      .partitionBy(classPartitioner).persist()

    val subjectCount = countDistSubjects(triplesByClass)

    val datasetId = triples.context.getConf.get("datasetId")

    subjectCount
      .coalesce(triples.context.defaultMinPartitions)
      .map {
        case (n,e) => ClssStats(datasetId, n, e)
      }
  }

  /* scrape General Stats */

  def scrapeGeneral() : RDD[Stats] = {

    val cnt = triples.count()
    val prp = triples.map(_.getPredicate).distinct().count()
    val ent = triples.filter(_.getPredicate.equals(u(rdf, "type"))).map(_.getSubject).distinct().count()
    val cls = triples.filter(_.getPredicate.equals(u(rdf, "type"))).map(_.getObject).distinct().count()
    val sjc = triples.map(_.getSubject).distinct().count()
    val ojc = triples.map(_.getObject).distinct().count()

    val endpoint = triples.context.getConf.get("sparqlEndpoint")
    val datasetid = triples.context.getConf.get("datasetId")

    triples.context.makeRDD(Seq(GenStats(datasetid, endpoint, cnt, prp, cls, ent, sjc, ojc)))
  }

  /* scrape main function */

  def scrape(flags: String) : RDD[Stats] = {

    val emptyrdd = triples.context.emptyRDD[Stats]
    val u = scrapeGeneral()
    val v = if (flags.contains("p")) {
      if (flags.contains("v"))
        scrapePredicatesVoc()
      else
        scrapePredicates()
    }
    else emptyrdd
    val x = if (flags.contains("p")) scrapeClasses()  else emptyrdd
    val y = if (flags.contains("s")) scrapeSubjects() else emptyrdd
    val z = if (flags.contains("o")) scrapeObjects()  else emptyrdd

    u.union(v).union(x).union(y).union(z)
  }
}


