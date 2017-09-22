package org.semagrow.sevod.scraper.io

import java.io.{DataInputStream, DataOutputStream}

import org.semagrow.sevod.model._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Writable}
import org.apache.jena.hadoop.rdf.io.input.ntriples.{BlockedNTriplesInputFormat, NTriplesInputFormat}
import org.apache.jena.hadoop.rdf.io.output.ntriples.NTriplesOutputFormat
import org.apache.jena.hadoop.rdf.types.{QuadWritable, TripleWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{Triple => JTriple}
import org.apache.spark.serializer.KryoRegistrator
import org.semagrow.sevod.scraper.io.JenaKryoSerializers._
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.jena.hadoop.rdf.io.input.nquads.BlockedNQuadsInputFormat
import org.semagrow.sevod.scraper.Statistics._


/**
  * Created by angel on 26/7/2016.
  */
object TriplesIOOps {

  class JenaKryoRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Array[org.apache.jena.graph.Node]], new NodeArraySerializer)
      kryo.register(classOf[org.apache.jena.graph.Node_Blank], new BlankNodeSerializer)
      kryo.register(classOf[org.apache.jena.graph.Node_ANY], new ANYNodeSerializer)
      kryo.register(classOf[org.apache.jena.graph.Node_Variable], new VariableNodeSerializer)
      kryo.register(classOf[org.apache.jena.graph.Node_URI], new URINodeSerializer)
      kryo.register(classOf[org.apache.jena.graph.Node_Literal], new LiteralNodeSerializer)
      kryo.register(classOf[org.apache.jena.graph.Triple], new TripleSerializer)
      kryo.register(classOf[Array[org.apache.jena.graph.Triple]])
      kryo.register(classOf[TripleWritable], new KryoWritableSerializer[TripleWritable])
      kryo.register(classOf[VoidStats])
      kryo.register(classOf[ClssStats])
      kryo.register(classOf[PredStats])
      kryo.register(classOf[PrefixStats])
      kryo.register(classOf[GenStats])
      kryo.register(classOf[TriplifierOfTriplifyable[VoidStats]])
      kryo.register(classOf[TriplifierOfTriplifyable[ClssStats]])
      kryo.register(classOf[TriplifierOfTriplifyable[PredStats]])
      kryo.register(classOf[TriplifierOfTriplifyable[PrefixStats]])
      kryo.register(classOf[TriplifierOfTriplifyable[GenStats]])
    }
  }

  /** A Kryo serializer for Hadoop writables. copied from Shark sources */
  class KryoWritableSerializer[T <: Writable] extends Serializer[T] {
    override def write(kryo: Kryo, output: Output, writable: T) {
      val ouputStream = new DataOutputStream(output)
      writable.write(ouputStream)
    }

    override def read(kryo: Kryo, input: Input, cls: java.lang.Class[T]): T = {
      val writable = cls.newInstance()
      val inputStream = new DataInputStream(input)
      writable.readFields(inputStream)
      writable
    }
  }

  class TriplesOutputOps[T] (rdd: RDD[T], triplifier : Triplifier[T]) {


    def saveAsNTriplesFile(path: String) : Unit = {
      this.saveAsNTriplesFile(path,triplifier)
    }

    def saveAsNTriplesFile(path: String, triplifier : Triplifier[T]) : Unit = {
      rdd.flatMap(v => triplifier.triplify(v).map(tv => new TripleWritable(tv)))
        .map(v => (1,v))
        .saveAsNewAPIHadoopFile(path,
          classOf[LongWritable],
          classOf[TripleWritable],
          classOf[NTriplesOutputFormat[LongWritable]])
    }
  }

  class TriplesInputOps (sc : SparkContext) {

    val conf = new Configuration()

    conf.set("rdf.io.input.ignore-bad-tuples", "true")
    conf.set("mapreduce.input.lineinputformat.linespermap", "100000")
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    def inputFile(path: String): RDD[JTriple] = {
      if (path.endsWith(".nt")) {
        nTriplesFile(path)
      }
      else if (path.endsWith(".nq")) {
        nQuadsFile(path)
      }
      else {
        sc.emptyRDD
      }
    }

    def nTriplesFile(path : String): RDD[JTriple] = {

      sc.newAPIHadoopFile(path,
          classOf[BlockedNTriplesInputFormat],
          classOf[LongWritable], //position
          classOf[TripleWritable], //value
          conf)
        .map(_._2.get())
    }

    def nQuadsFile(path : String): RDD[JTriple] = {

      sc.newAPIHadoopFile(path,
          classOf[BlockedNQuadsInputFormat],
          classOf[LongWritable], //position
          classOf[QuadWritable], //value
          conf)
        .map(_._2.get())
        .map(_.asTriple())
    }
  }

  implicit def sparkContextToTriplesInput(sc: SparkContext) : TriplesInputOps = new TriplesInputOps(sc)

  implicit def rddTriplesToTriplesOutput[T : Triplifier](rdd: RDD[T]) : TriplesOutputOps[T] = new TriplesOutputOps(rdd, implicitly[Triplifier[T]])

}
