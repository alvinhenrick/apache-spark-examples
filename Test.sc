

val Regex = """^\[\[(.+?)\]\].(.*)\s.*(https?://[^\s]+).*[*].*(.*?).*[*].*\((.*?)\)(.*)$""".r

val line = "[[07Q642678114]]4 B10 02https://www.wikidata.org/w/index.php?diff=330664519&oldid=318565522&rcid=340722685 5* 03Edoderoobot 5* (+76) 10/* wbeditentity-update:0| */ nl-description, python code on https://goo .gl/QsntSb, logfile on https://goo .gl/BezTim"


val out = line match {
  case Regex(title, flags, user, diffUrl, byteDiff, summary) => (title, flags, user, diffUrl, byteDiff, summary)
  case _ => None
}







