package org.semagrow.sevod.model

import org.apache.jena.graph._

import scala.reflect.ClassTag


/**
  * Created by angel on 26/7/2016.
  */
trait Triplifyable[T] {

    def triplify() : Seq[Triple]

}


trait Triplifier[T] {

  def triplify(t : T) : Seq[Triple]

}

class TriplifierOfTriplifyable[T <: Triplifyable[T]] extends Serializable with Triplifier[T] {

  def triplify(t : T) = t.triplify()

}

object TriplifierImplicits {

  implicit def triplifierOfTriplifyable[T <: Triplifyable[T]](implicit c : ClassTag[T]) : Triplifier[T] = new TriplifierOfTriplifyable[T]()

}

//implicit val triplifier : Triplifier[T] = new TriplifierOfTriplifyable()

//implicit def triplifierOfTriplifyable[T <: Triplifyable[T]]() : Triplifier[T] = new TriplifierOfTriplifyable()