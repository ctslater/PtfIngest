
package ptf

import scala.math.{pow, sqrt}
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

object ObjectSet {

  type RaDec = (Double, Double)
  type ObjectId = Long
  type SourceId = Long

}

class ObjectSet {

  import ObjectSet.{RaDec, SourceId, ObjectId}

  var objects: HashMap[RaDec, ObjectId] = HashMap()
  var sourceToObjectList: ListBuffer[Seq[(SourceId, ObjectId)]] = new ListBuffer

  def sourceToObject: Map[SourceId, ObjectId] = {
    sourceToObjectList.flatten.toMap
  }

  // Singleton that manages new object IDs
  object objIdCounter {
    var id: ObjectId = 0
    def getNext: ObjectId = {
      id = id + 1
      id
    }
  }

  def addNewSources(sources: Seq[(RaDec, SourceId)]) : Unit = {
    val newMatches = findMatches(sources, objects.toSeq)

    val sourceIdSet = sources.map(_._2).toSet
    val unmatchedSourceIds = sourceIdSet.diff(newMatches.keys.toSet)
    val newObjects: Map[SourceId, ObjectId] = unmatchedSourceIds.map(
      sourceId => (sourceId -> objIdCounter.getNext )).toMap

    // Update the sourceToObject maping with new objects
    sourceToObjectList += newMatches.toSeq
    sourceToObjectList += newObjects.toSeq

    // Add newly-generated objects to the object tree
    objects = objects ++ sources.collect({
      case (coord, sourceId) if unmatchedSourceIds(sourceId)
        => (coord, newObjects(sourceId)) })

  }

  def findNearestAcceptable[V](inputSet: Seq[(RaDec, V)], target: RaDec,
                               tolerance: Double): Option[V] = {

    def sqDistance(a: (Double, Double), b: (Double, Double)): Double = {
      pow(a._1 - b._1, 2) + pow(a._2 - b._2, 2)
    }

    def findMinimumDist(a: ((Double, Double), V) , b: ((Double, Double), V)) = {
      if (sqDistance(a._1, target) < sqDistance(b._1, target)) a else b
    }

    val nearestMatch = inputSet.reduceOption(findMinimumDist(_, _))

    for {
      (nearestCoord, nearestValue) <- nearestMatch
      if (sqDistance(nearestCoord, target) < tolerance*tolerance)
    } yield nearestValue
  }

  def findMatches[V1, V2](set1: Seq[(RaDec, V1)],
                          set2: Seq[(RaDec, V2)],
                          tolerance: Double = 1.5/3600.0): Map[V1, V2] = {

    val map1to2: Map[V1, Option[V2]] = set1 map {
      case (target, id) => (id -> findNearestAcceptable(set2, target, tolerance)) } toMap
    val map2to1: Map[V2, Option[V1]] = set2 map {
      case (target, id) => (id -> findNearestAcceptable(set1, target, tolerance)) } toMap

    val okValuesSet1 = map1to2.mapValues({case Some(id) => map2to1(id)
                                              case None => None}).filter({case (id1, id2) => id2.contains(id1)})

    // Handshake matches can't have None
    val handshakeMatches = okValuesSet1.mapValues(id => map1to2(id.get).get)

    handshakeMatches
  }

}
