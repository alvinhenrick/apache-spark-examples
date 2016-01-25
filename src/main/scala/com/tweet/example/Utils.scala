package com.tweet.example

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

/**
 * Created by Alvin on 7/22/15.
 */
object Utils {

  val numFeatures = 1000
  val tf = new HashingTF(numFeatures)

  /**
   * Create feature vectors by turning each tweet into bigrams of
   * characters (an n-gram model) and then hashing those to a
   * length-1000 feature vector that we can pass to MLlib.
   * This is a common way to decrease the number of features in a
   * model while still getting excellent accuracy. (Otherwise every
   * pair of Unicode characters would potentially be a feature.)
   */
  def featurize(s: String): Vector = {
    tf.transform(s.sliding(2).toSeq)
  }

  def resolveGender(gender: String): Int = {
    gender match {
      case "M" => 1
      case "F" => 0
      case _ => -1
    }
  }

  def resolveState(state: String): Int = {
    state match {
      case "AL" => 1
      case "AK" => 2
      case "AZ" => 3
      case "AR" => 4
      case "CA" => 5
      case "CO" => 6
      case "CT" => 7
      case "DE" => 8
      case "FL" => 9
      case "GA" => 10
      case "HI" => 11
      case "ID" => 12
      case "IL" => 13
      case "IN" => 14
      case "IA" => 15
      case "KS" => 16
      case "KY" => 17
      case "LA" => 18
      case "ME" => 19
      case "MD" => 20
      case "MA" => 21
      case "MI" => 22
      case "MN" => 23
      case "MS" => 24
      case "MO" => 25
      case "MT" => 26
      case "NE" => 27
      case "NV" => 28
      case "NH" => 29
      case "NJ" => 30
      case "NM" => 31
      case "NY" => 32
      case "NC" => 33
      case "ND" => 34
      case "OH" => 35
      case "OK" => 36
      case "OR" => 37
      case "PA" => 38
      case "RI" => 39
      case "SC" => 40
      case "SD" => 41
      case "TN" => 42
      case "TX" => 43
      case "UT" => 44
      case "VA" => 45
      case "VT" => 46
      case "WA" => 47
      case "WV" => 48
      case "WI" => 49
      case "WY" => 50
      case _ => 51
    }
  }

}
