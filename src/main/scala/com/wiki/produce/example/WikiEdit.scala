package com.wiki.produce.example

import java.sql.Timestamp
import java.util.Date

/**
  * Created by shona on 5/9/16.
  */

object EditType extends Enumeration {
  type EditType = Value
  val SPECIAL, TALK, EDIT = Value
}

case class WikiEdit(channel: String, timestamp: Date, title: String, flags: String,
                    page: String, username: String, diff: String, comment: String,
                    isNew: Boolean, isMinor: Boolean, isUnpatrolled: Boolean, isBotEdit: Boolean,
                    editType: EditType.EditType)

object WikiEdit {

  def apply(channel: String, timestamp: Date, title: String, flags: String, page: String,
            username: String, diff: String, comment: String): WikiEdit =
    WikiEdit(channel, timestamp, title, flags, page, username, diff, comment,
      "N".contains(flags), "M".contains(flags), "B".contains(flags), "!".contains(flags),
      if (title.startsWith("Special:")) EditType.SPECIAL
      else if (title.startsWith("Talk:")) EditType.TALK
      else EditType.EDIT)

}
