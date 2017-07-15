
package ptf


case class PtfFilename(filename: String, date: String, timestamp: String,
                       serial: String, chip: String)

object PtfFilename {
  def apply(filename: String): Option[PtfFilename] = {
    // Example looks like: ./PTF_200912024227_c_p_scie_t100845_u011771826_f02_p101001_c09.parquet
    val pattern = """PTF_(\d+)_[\w_]*_t(\d+)_u(\d+)_[\w\d]+_[\w\d]+_c(\d+)""".r("date", "t", "u", "c").unanchored

    pattern.findFirstMatchIn(filename) match {
      case Some(matchResult) => Some(new PtfFilename(filename,
                                                     date=matchResult.group("date"),
                                                     timestamp=matchResult.group("t"),
                                                     serial=matchResult.group("u"),
                                                     chip=matchResult.group("c")))
      case None => None
    }
  }
}

case class CoordsIdZone(source_id: Long, visit: Long, ALPHAWIN_J2000: Double, DELTAWIN_J2000: Double, zone: Int)
case class CoordsId(source_id: Long, visit: Long, ALPHAWIN_J2000: Double, DELTAWIN_J2000: Double)
case class SrcObjMatches(source_id: Long, obj_id: Long, zone: Long)

object CoordsIdZone {
  def fromCoordsId(input: CoordsId, zone: Int) = {
    new CoordsIdZone(source_id = input.source_id,
                     visit = input.visit,
                     ALPHAWIN_J2000 = input.ALPHAWIN_J2000,
                     DELTAWIN_J2000 = input.DELTAWIN_J2000,
                     zone = zone)
  }
}



object PtfIngest {


  def assign_zones(row: CoordsId): Seq[CoordsIdZone] = {

    def computeZone(dec: Double, height: Double, buffer: Double): Seq[Int] =  {
      Seq(-buffer, 0, buffer).map(offset => ((dec + offset + 90)/height).ceil.toInt).distinct
    }

    val height = 60/3600.0
    val buffer = 10/3600.0

    computeZone(row.DELTAWIN_J2000, height, buffer).map(zone => CoordsIdZone.fromCoordsId(row, zone))
  }
  /*
   *
   * Matching Functions
   *
   */

  def match_within_zone(zone: Int, rows: Iterator[CoordsIdZone]): Iterable[SrcObjMatches] = {

    val sourceTuples = rows.toList.groupBy(_.visit).map({
      case (visit, visit_rows) => visit_rows.map(row => ((row.ALPHAWIN_J2000, row.DELTAWIN_J2000), row.source_id))})

    val objects = new ObjectSet
    sourceTuples foreach objects.addNewSources _

    objects.sourceToObject.map({case (src_id, obj_id) => SrcObjMatches(src_id, zone*10000 + obj_id, zone)})
  }

}
