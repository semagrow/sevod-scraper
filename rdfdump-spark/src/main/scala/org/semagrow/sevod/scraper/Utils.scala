package org.semagrow.sevod.scraper

import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{Node, NodeFactory, Triple}

/**
  * Created by antonis on 30/8/2017.
  */
class Utils {

}


object Utils {
  // Helpers

  def t(s: Node, p : Node, o: Node) = new Triple(s,p,o)
  def u(s: String) = NodeFactory.createURI(s)
  def u(p:String, s: String) = NodeFactory.createURI(p.concat(s))
  def l(o : Long) = NodeFactory.createLiteralByValue(o, XSDDatatype.XSDinteger)
  def l(o : String) = NodeFactory.createLiteral(o)
  def bn() = NodeFactory.createBlankNode()
  def bn(s : String) = NodeFactory.createBlankNode(s)

  // Namespaces

  val vd = "http://rdfs.org/ns/void#"
  val svd = "http://rdf.iit.demokritos.gr/2013/sevod#"
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}
