package com.twitter.example

/**
 * Created by Alvin on 5/20/15.
 */

import java.io.StringReader

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.util.Version

import scala.collection.mutable

object Tokenizer {
  val LuceneVersion = Version.LUCENE_5_1_0

  def tokenizeAll(docs: Iterable[Document]) = docs.map(tokenize)

  def tokenize(doc: Document): TermDoc = TermDoc(doc.docId, doc.labels, tokenize(doc.body))

  def tokenize(content: String): Seq[String] = {
    val tReader = new StringReader(content)
    val analyzer = new EnglishAnalyzer()
    val tStream = analyzer.tokenStream("contents", tReader)
    val term = tStream.addAttribute(classOf[CharTermAttribute])
    tStream.reset()

    val result = mutable.ArrayBuffer.empty[String]
    while (tStream.incrementToken()) {
      val termValue = term.toString
      if (!(termValue matches ".*[\\d\\.].*")) {
        result += term.toString
      }
    }
    result
  }
}

case class TermDoc(doc: String, labels: Set[String], terms: Seq[String])