package com.tweet.example

/**
 * Created by Alvin on 5/20/15.
 */
object TextParser {

  val Regex = """^([^"]|"[^"]*")*?[,]([^"]|"[^"]*")*?[,]([^"]|"[^"]*")*?[,](.+)$""".r

  def parseAll(lines: Iterator[String]) = lines map parse

  def parse(line: String) = line match {

    case Regex(itemId, sentiment, sentimentSource, sentimentText) => Some(Document(itemId, sentimentText, sentiment.toDouble))
    case _ => None

  }
}

case class Document(docId: String, body: String = "", label: Double)

