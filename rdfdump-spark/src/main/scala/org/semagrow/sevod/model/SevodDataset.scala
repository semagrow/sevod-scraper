package org.semagrow.sevod.model

import org.apache.jena.iri.IRI

/**
  * Created by angel on 26/7/2016.
  */
trait VoidDataset {

  val triples: Long
  val distinctSubjects: Long
  val distinctObject: Long
  val distinctPredicates: Long

  val subjectVocab : Seq[IRI]
  val objectVocab  : Seq[IRI]
  val predicates   : Seq[IRI]

}


trait SevodDataset extends VoidDataset

