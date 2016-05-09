name := "dfw-spark-meetup"

version := "0.0.1"

scalaVersion := "2.10.5"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "com.databricks" %% "spark-csv" % "1.3.0",
  "org.json4s" %% "json4s-native" % "3.2.10",

  "pircbot" % "pircbot" % "1.5.0",

  // Lucene
  "org.apache.lucene" % "lucene-core" % "5.1.0",
  // for Porter Stemmer
  "org.apache.lucene" % "lucene-analyzers-common" % "5.1.0"

  //"org.apache.hadoop" % "hadoop-common" % "2.6.0" % "provided"

)
ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

/*val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case _ => MergeStrategy.first
}*/