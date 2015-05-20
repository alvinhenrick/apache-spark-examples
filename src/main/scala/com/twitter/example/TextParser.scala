package com.twitter.example

/**
 * Created by Alvin on 5/20/15.
 */
object TextParser {

  val Regex = """^([^"]|"[^"]*")*?[,]([^"]|"[^"]*")*?[,]([^"]|"[^"]*")*?[,](.+)$""".r

  def parseAll(lines: Iterator[String]) = lines map parse

  def parse(line: String) = line match {

    case Regex(itemId, sentiment, sentimentSource, sentimentText) => Some(Document(itemId, sentimentText, Set(sentimentToString(sentiment.toInt))))
    case _ => None
    //ItemID,Sentiment,SentimentSource,SentimentText
    /*if (line("Sentiment").toInt == 0) {
      Document(line("ItemID"), line("SentimentText"), Set("negative"))
    } else {
      Document(line("ItemID"), line("SentimentText"), Set("positive"))
    }*/
  }

  def sentimentToString(sentiment: Int) = sentiment match {
    case 0 => "negative"
    case 1 => "positive"
    case _ => "unknown"
  }
}

case class Document(docId: String, body: String = "", labels: Set[String] = Set.empty)
