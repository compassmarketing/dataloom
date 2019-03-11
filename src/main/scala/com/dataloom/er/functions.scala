package com.dataloom.er

import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.commons.text.similarity.{JaroWinklerDistance, LevenshteinDistance}
import org.apache.spark.ml.linalg.Vector

object functions {

  def jaccardDistance[T](x: Seq[T], y: Seq[T]): Double = {
    val xSet = x.toSet
    val ySet = y.toSet
    val intersectionSize = xSet.intersect(ySet).size.toDouble
    val unionSize = xSet.size + ySet.size - intersectionSize
    assert(unionSize > 0, "The union of two input sets must have at least 1 elements")
    1 - intersectionSize / unionSize
  }

  def jaccardContainment(x: Vector, y: Vector): Double = {
    val xSet = x.toSparse.indices.toSet
    val ySet = y.toSparse.indices.toSet
    val intersectionSize = xSet.intersect(ySet).size.toDouble
    assert(xSet.nonEmpty, "The first set must have at least 1 elements")
    val xCont = intersectionSize / xSet.size
    val yCont = intersectionSize / ySet.size
    Math.max(xCont, yCont)
  }

  def levDistance(nameA: String, nameB: String): Double = {
    val lev = new LevenshteinDistance()
    lev.apply(nameA, nameB).toDouble
  }

  def jaroWrinler(nameA: String, nameB: String): Double = {
    val jaroWinkler = new JaroWinklerDistance()
    jaroWinkler.apply(nameA, nameB)
  }

  def doubleMetaBinary(nameA: String, nameB: String): Double = {
    val dblMeta = new DoubleMetaphone()
    if (dblMeta.encode(nameA) == dblMeta.encode(nameB)) 1.0 else 0.0
  }

  def givenNameScore(nameA: String, nameB: String): Double = {
    val a = helpers.removePunctuation(nameA)
    val b = helpers.removePunctuation(nameB)

    // If names equal, return
    if (a == b) return 1.0

    // If either name is blank, then no match
    if (a.length < 1 || b.length < 1) return 0.0

    val aTokens = a.split(" ").flatMap ( t => if (t.length == 2) Array(t.charAt(0).toString, t.charAt(1).toString) else Array(t) )
    val bTokens = b.split(" ").flatMap ( t => if (t.length == 2) Array(t.charAt(0).toString, t.charAt(1).toString) else Array(t) )

    val aTokenSize = aTokens.foldRight(0)( (v, sum) => sum + v.length)
    val bTokenSize = bTokens.foldRight(0)( (v, sum) => sum + v.length)

    // Token contain initials
    if (aTokens.length == bTokens.length && (aTokenSize == aTokens.length || bTokenSize == bTokens.length)) {
      val aInitials = aTokens.foldRight("")( (v, n) => n + v.charAt(0) )
      val bInitials = bTokens.foldRight("")( (v, n) => n + v.charAt(0) )

      if (aInitials.equals(bInitials)) 1.0 else 0.0
    }
    else{
      jaroWrinler(a, b)
    }

  }

  def familyNameScore(nameA:String, nameB:String):Double = {
    jaroWrinler(helpers.removePunctuation(nameA), helpers.removePunctuation(nameB))
  }

  def genderClassification(genderA:String, genderB:String):Int = {
    (genderA, genderB) match {
      case ("F", "F") => 1
      case ("M", "M") => 1
      case ("M", "F") => 2
      case ("F", "M") => 2
      case _ => 0
    }
  }


}

private[er] object helpers {

  def removePunctuation(a: String): String = a.replaceAll("""[\p{Punct}]""", "")


}
