
package ptfBenchmark

import scala.util.Random
import ptf.PtfIngest.match_within_zone
import ptf.{CoordsIdZone, SrcObjMatches}


class ObjectMatchingBenchmark {

  def runMatching: Iterable[SrcObjMatches] = {

    val zone = 5
    val rows: Seq[CoordsIdZone] = randomPoints
    match_within_zone(zone, rows.toIterator)
  }

  def randomPoints: Seq[CoordsIdZone] = {
    val rand = new Random
    val raDec = (1 to 1000).map{ _ => (rand.nextFloat(), rand.nextFloat()) }
    (1 to 100).flatMap { visit =>
      raDec.map { case (ra, dec) =>
        CoordsIdZone(source_id=rand.nextLong(), visit=visit,
                    ALPHAWIN_J2000=ra, DELTAWIN_J2000=dec, zone=5)
      }
    }
  }

}
