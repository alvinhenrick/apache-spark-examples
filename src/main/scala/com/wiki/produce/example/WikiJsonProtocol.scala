package com.wiki.produce.example

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
  * Created by shona on 5/9/16.
  */

object WikiJsonProtocol extends DefaultJsonProtocol {
  implicit val wikiFormat: RootJsonFormat[WikiEdit] = jsonFormat(WikiEdit.apply, "channel", "timestamp", "title", "flags",
    "page", "username", "diff", "comment")
}
