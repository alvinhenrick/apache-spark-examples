package com.twitter.example

/**
 * Created by shona on 4/30/15.
 */

import java.io.Serializable

import com.google.common.collect.ImmutableBiMap
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.JavaConversions._

class Dictionary(dict: Seq[String]) extends Serializable {

  // map term => index
  val termToIndex = ImmutableBiMap.builder[String, Int]()
    .putAll(dict.zipWithIndex.toMap[String, Int])
    .build()

  @transient
  lazy val indexToTerm = termToIndex.inverse()

  val count = termToIndex.size()

  def indexOf(term: String) = termToIndex(term)

  def valueOf(index: Int) = indexToTerm(index)

  def tfIdfs(terms: Seq[String], idfs: Map[String, Double]) = {
    val filteredTerms = terms.filter(idfs contains)
    filteredTerms.groupBy(identity).map {
      case (term, instances) =>
        (indexOf(term), (instances.size.toDouble / filteredTerms.size.toDouble) * idfs(term))
    }.toSeq.sortBy(_._1) // sort by termId
  }

  def vectorize(tfIdfs: Iterable[(Int, Double)]) = Vectors.sparse(dict.size, tfIdfs.toSeq)
}