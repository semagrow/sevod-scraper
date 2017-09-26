package org.semagrow.sevod.scraper

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.semagrow.sevod.model.Triplifyable

/**
  * Created by antonis on 30/8/2017.
  */
class Statistics {

}

object Statistics {

  import Utils._

  trait Stats extends Triplifyable[Stats]

  case class VoidStats(endpoint: String,
                       property: Node,
                       count : Long,
                       distinctSubjects: Long,
                       distinctObjects: Long) extends Stats {

    override def triplify(): Seq[Triple] = {
      val dataset = getDataset(endpoint)
      val n = NodeFactory.createBlankNode()
      Seq(
        t(dataset, u(vd,"propertyPartition"), n),
        t(n, u(vd,"property"), property),
        t(n, u(vd,"triples"), l(count)),
        t(n, u(vd,"distinctSubjects"), l(distinctSubjects)),
        t(n, u(vd,"distinctObjects"), l(distinctObjects))
      )
    }
  }

  case class ClssStats(endpoint: String,
                       clazz: Node,
                       entities : Long) extends Stats {

    override def triplify(): Seq[Triple] = {
      val dataset = getDataset(endpoint)
      val n = NodeFactory.createBlankNode()
      Seq(
        t(dataset, u(vd,"classPartition"), n),
        t(n, u(vd,"class"), clazz),
        t(n, u(vd,"entities"), l(entities))
      )
    }
  }

  case class PredStats(voidStats: VoidStats,
                       subjectVocab: Option[Iterable[Node]],
                       objectVocab: Option[Iterable[Node]]) extends Stats {

    def triplifyVocabularies(sub: Node, vocabularies: Option[Iterable[Node]]): Seq[Triple] = vocabularies match {
      case Some(_) => vocabularies.get.map(n => t(sub, u(svd,"subjectVocabulary"), n)).toSeq
      case None => Seq()
    }

    override def triplify(): Seq[Triple] = {
      val vst = voidStats.triplify()
      val sub = vst.head.getObject
      val sst = triplifyVocabularies(sub, subjectVocab)
      val ost = triplifyVocabularies(sub, objectVocab)
      vst ++ sst ++ ost
    }
  }

  case class PrefixStats(endpoint: String,
                         whatPrefix : String,
                         thePrefix : String,
                         count : Long,
                         distinctSubjects: Long,
                         distinctObjects: Long) extends Stats {

    override def triplify(): Seq[Triple] = {
      val dataset = getDataset(endpoint)
      val n = NodeFactory.createBlankNode()
      Seq(
        t(dataset, u(vd,"subset"), n),
        t(n, u(vd, whatPrefix ++ "RegexPattern"), l(thePrefix)),
        t(n, u(vd,"triples"), l(count)),
        t(n, u(vd,"distinctSubjects"), l(distinctSubjects)),
        t(n, u(vd,"distinctObjects"), l(distinctObjects))
      )
    }
  }

  case class GenStats(endpoint: String,
                      triples: Long,
                      properties: Long,
                      classes: Long,
                      entities: Long,
                      distinctSubjects: Long,
                      distinctObjects: Long) extends Stats {

    override def triplify(): Seq[Triple] = {
      val dataset = getDataset(endpoint)
      Seq(
        t(dataset, u(rdf,"type"), u(vd,"Dataset")),
        t(dataset, u(vd,"sparqlEndpoint"), u(endpoint)),
        t(dataset, u(vd,"triples"), l(triples)),
        t(dataset, u(vd,"properties"), l(properties)),
        t(dataset, u(vd,"classes"), l(classes)),
        t(dataset, u(vd,"entities"), l(entities)),
        t(dataset, u(vd,"distinctSubjects"), l(distinctSubjects)),
        t(dataset, u(vd,"distinctObjects"), l(distinctObjects))
      )
    }
  }

  def getDataset(endpoint: String) = {
    def hash(s: String) = {
      val m = java.security.MessageDigest.getInstance("MD5")
      val b = s.getBytes("UTF-8")
      m.update(b, 0, b.length)
      new java.math.BigInteger(1, m.digest()).toString(16)
    }
    NodeFactory.createBlankNode(hash(endpoint))
  }
}