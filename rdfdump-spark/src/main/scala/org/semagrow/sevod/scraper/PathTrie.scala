package org.semagrow.sevod.scraper

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

/**
  * Created by antonis on 30/8/2017.
  */

class PathTrie {}

object PathTrie {

  val maxTrieHeight = 1000

  def getPrefixes(uris: RDD[String], trieParameter: Int) : RDD[String] = {

    val trie : RDD[((Int, String),Seq[String])] = {
      val list = uris
        .distinct()
        .map(_.substring("http://".size))
        .flatMap(s => (List.range(0, maxTrieHeight).zip((List("http:/") ++ s.split("/"))).zip(s.split("/"))))
        .partitionBy(new HashPartitioner(uris.context.defaultParallelism))
        .distinct()
        .persist()

      val filteredPrefixes = list
        .mapValues(x => 1)
        .reduceByKey(_+_)
        .filter(_._2 < trieParameter)
        .join(list)
        .mapValues(_._2)
        .mapValues(n => Seq(n))
        .reduceByKey(_ ++ _)

      filteredPrefixes
    }

    def atLevel(level: Int) = {
      trie
        .filter { case ((l, x), xs) => l == level }
        .flatMap { case ((l, x), xs) => xs.map(y => (x, y)) }
    }

    def concatStr(p : String, q: String) : String =
      if (q == null) p else  p ++ "/" ++ q

    def transform(stream: RDD[(String, String)]) = {
      stream
        .map { case (x, y) => (y, concatStr(x,y)) }
        .keyBy(_._1).mapValues(_._2)
    }

    def afterJoin(stream: RDD[(String, (String, Option[String]))]) = {
      stream
        .map {
          case (x, (y, Some(z))) => (y, z)
          case (x, (y, None)) => (y, null)
        }
        .keyBy(_._1).mapValues(_._2)
    }

    Range(1, maxTrieHeight)
      .map(i => atLevel(i))
      .takeWhile(s => (!(s.isEmpty)))
      .foldLeft(atLevel(0)) {
        case (l, r) => afterJoin(transform(l).leftOuterJoin(r))
      }
      .map { case (x,y) => concatStr(x,y) }
  }
}
