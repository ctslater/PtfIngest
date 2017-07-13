
package ptf

import com.thesamet.spatial.{ KDTreeMap, Metric, DimensionalOrdering }
import scala.math.{pow, sqrt}

object ObjectSet {

  type RaDec = (Double, Double)
  type ObjectId = Long
  type SourceId = Long

}

class ObjectSet {

  import ObjectSet.{RaDec, SourceId, ObjectId}

  var objectTree: KDTreeMap[RaDec, ObjectId] = KDTreeMap()
  val sourceToObject: Map[SourceId, ObjectId] = Map()
  var maxObjId: Int = 0

  def addNewSources(sources: Seq[(RaDec, SourceId)]) : Unit = {
    val newSourceTree: KDTreeMap[RaDec, SourceId] = KDTreeMap.fromSeq(sources)
    val newMatches = find_matches(newSourceTree, objectTree)

    val sourceIdSet = sources.map(_._2).toSet
    val unmatchedIds = sourceIdSet.diff(newMatches.keys.toSet)


    // Need to assign object ids to unmatched sources

    // Currently assigning the wrong ID to this, bad.
    objectTree = KDTreeMap.fromSeq(objectTree.toSeq ++ sources.filter({case (coord, id) => unmatchedIds(id)}))
    println(sourceIdSet, unmatchedIds)


    // For handshakes, add
    // sourceToObject = sourceToObject ++ newMatches
  }

  def findNearestAcceptable[V](tree: KDTreeMap[RaDec, V], key: (Double, Double),
                               tolerance: Double): Option[V] = {

    def distance(a: (Double, Double), b: (Double, Double)) = {
      sqrt( pow(a._1 - b._1, 2) + pow(a._2 - b._2, 2))
    }

    def keep_minimum_dist(a: ((Double, Double), V) , b: ((Double, Double), V)) = {
      if (distance(a._1, key) < distance(b._1, key)) a else b
    }

    val candidates = tree.findNearest(key, n=4)
    val nearestMatch = candidates.reduceRightOption(keep_minimum_dist(_, _))

    for {
      (nearest_coord, nearest_val) <- nearestMatch
      if (distance(nearest_coord, key) < tolerance)
    } yield nearest_val
  }

  def find_matches[V1, V2](tree1: KDTreeMap[(Double, Double), V1],
                           tree2: KDTreeMap[(Double, Double), V2], tolerance: Double = 1.5/3600.0): Map[V1, V2] = {

    val map_1to2 = tree1 map { case (key, id) => (id -> findNearestAcceptable(tree2, key, tolerance)) }
    val map_2to1 = tree2 map { case (key, id) => (id -> findNearestAcceptable(tree1, key, tolerance)) }

    val tree1_handshakes = map_1to2.mapValues({case Some(id) => map_2to1(id)
                                               case None => None}).filter({case (id1, id2) => id2.contains(id1)})

    // Handshake matches can't have None
    val handshake_matches = tree1_handshakes.mapValues(id => map_1to2(id.get).get)

    // Eventually this will be combined with unmatched objects
    handshake_matches
  }

}