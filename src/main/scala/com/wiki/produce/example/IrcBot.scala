package com.wiki.produce.example

/**
  * Created by shona on 5/3/16.
  */

import java.sql.Timestamp

import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver
import org.jibble.pircbot.PircBot

import scala.util.Random

/** Custom IRC bot that parses edit messages from the IRC edit stream */
private class IrcBot(
                      receiver: Receiver[WikiEdit],
                      channels: Seq[String],
                      server: String,
                      port: Int = 6667) extends PircBot with Logging with Serializable {

  val Regex = """^.*\[\[(.+?)\]\].\s(.*)\s.*(https?://[^\s]+).*[*]\s(.*?)\s.*[*]\s\((.*?)\)(.*)$""".r

  val nick = s"sparkstream-${Random.nextLong()}"

  def start(): Unit = {
    logInfo("starting IRC bot")
    setName(nick)
    setLogin(nick)
    setFinger("")
    setVerbose(true)
    setEncoding("UTF-8")
    connect(server, port)

    channels foreach joinChannel

    logInfo(s"IRC bot connected to $server")
  }

  override def onMessage(channel: String, sender: String, login: String, hostname: String, message: String): Unit = {
    processMessage(message, channel.substring(1), System.currentTimeMillis()) foreach receiver.store
  }

  private def processMessage(line: String, channel: String, timestamp: Long): Option[WikiEdit] = try {
    val input = line.replaceAll("[^\\x20-\\x7E]", "")
    input match {
      case Regex(title, flags, diffUrl, user, byteDiff, summary) => Some(WikiEdit(channel, new Timestamp(timestamp),
        title, flags, diffUrl, user, byteDiff, summary))
      case _ => None
    }


  } catch {
    case ex: Throwable => {
      logWarning(s"could not parse input line: $line, error $ex")
      None
    }
  }
}
