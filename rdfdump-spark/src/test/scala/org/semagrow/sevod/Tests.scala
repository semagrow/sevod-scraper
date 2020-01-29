package org.semagrow.sevod

import junit.framework.TestCase
import org.apache.spark.{SparkConf, SparkContext}
import org.semagrow.sevod.scraper.PathTrie
import org.junit.Assert._

/**
  * Created by antonis on 28/8/2017.
  */

class Tests extends TestCase {

  def testUriPrunner = {

    val uris = Seq(
      "http://www.w3.org/2000/01/rdf-schema#isDefinedBy",
      "http://data.nytimes.com/elements/topicPage",
      "http://data.nytimes.com/N75175044420491622923",
      "http://data.nytimes.com/82730566099810429733",
      "http://data.nytimes.com/86016435089497300863",
      "http://data.nytimes.com/63976031711474929253",
      "http://data.nytimes.com/69271848381811133523",
      "http://data.nytimes.com/N36119714487301414133",
      "http://data.nytimes.com/N13463766342949717891",
      "http://data.nytimes.com/N62294250482713939931",
      "http://topics.nytimes.com/top/reference/timestopics/people/f/nick_faldo/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/r/eleanor_roosevelt/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/m/barbara_a_mikulski/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/k/deena_kastor/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/j/james_l_jones/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/b/tim_burton/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/w/rusty_wallace/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/c/ted_cottrell/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/d/charles_dutoit/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/a/shoko_asahara/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/z/frank_zappa/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/c/geoffrey_canada/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/m/susan_molinari/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/f/robert_h_frank/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/d/lou_dobbs/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/g/nomar_garciaparra/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/s/donald_siegelman/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/h/william_hurt/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/h/john_hume/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/s/richard_m_scrushy/index.html",
      "http://topics.nytimes.com/top/reference/timestopics/people/h/masaru_hayami/index.html",
      "http://localhost:8890/DAV//.rss2atom.xsl",
      "http://localhost:8890/DAV//.folder.xsl",
      "http://localhost:8890/DAV//.myfolder.xsl",
      "http://localhost:8890/DAV//.xml2rss.xsl",
      "http://localhost:8890/DAV//.rss2atom.xsl",
      "http://localhost:8890/DAV//.rss2rdf.xsl",
      "http://localhost:8890/DAV//home/",
      "http://localhost:8890/DAV//VAD/",
      "http://localhost:8890/DAV//VAD/conductor/",
      "http://localhost:8890/DAV//VAD/conductor/account_create.vspx"
    )

    val result = Set(
      "http://data.nytimes.com",
      "http://topics.nytimes.com/top/reference/timestopics/people",
      "http://www.w3.org/2000/01/rdf-schema#isDefinedBy",
      "http://localhost:8890/DAV/"
    )

    val sparkConfig = new SparkConf()
      .setMaster("local[*]")
      .setAppName("testUriPrunner")

    val sc = new SparkContext(sparkConfig)

    val urirdd = sc.makeRDD(uris)

    assertEquals(PathTrie.getPrefixes(urirdd, 7).collect().toSet, result)

  }
}
