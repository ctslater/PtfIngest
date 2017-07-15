
package ptf

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.functions.avg
import java.io.File
import PtfIngest._

/*
 * Program Entry
 */

object PtfIngestMain {

  val spark = SparkSession.builder().appName("PtfIngest").getOrCreate()
  import spark.implicits._

  def df_with_source_id(ptfFile: PtfFilename): DataFrame = {
    import org.apache.spark.sql.functions.lit
    val df = spark.read.parquet(ptfFile.filename)
    df.withColumn("source_id", df("number") + 10000*ptfFile.serial.toLong)
      .withColumn("visit", lit(ptfFile.date.toLong))
  }

  def main(args: Array[String]) {

    val data_dir = new File(args(0))

    if(data_dir.listFiles == null) {
      println(s"Data directory ${args(0)} is invalid")
      System.exit(1)
    }

    val ptf_files = data_dir.listFiles.map(_.toString).map(PtfFilename(_)).flatten


    val ok_keys = Seq("source_id", "visit", "ALPHAWIN_J2000", "DELTAWIN_J2000",
      "MAG_BEST", "MAGERR_BEST",
      "FLUX_AUTO", "FLUXERR_AUTO",
      "FLAGS", "CLASS_STAR", "ZEROPOINT")

    val small_subset = true

    val all_dfs = if (small_subset) {
      val unique_dates = ptf_files.map(_.date).distinct
      val exposure1_files = ptf_files.filter(_.date == unique_dates(0))
      val exposure1_df = exposure1_files.map(df_with_source_id).reduceRight(_.union(_))

      val exposure2_files = ptf_files.filter(_.date == unique_dates(1))
      val exposure2_df = exposure2_files.map(df_with_source_id).reduceRight(_.union(_))
      exposure1_df.union(exposure2_df)
    } else
      ptf_files.map(df_with_source_id).reduceRight(_.union(_)).coalesce(30)

    val zone_df = all_dfs.as[CoordsId].flatMap(assign_zones(_))
    val matches = zone_df.groupByKey(_.zone).flatMapGroups(match_within_zone)

    val matched_data = matches.join(all_dfs.select(cols=ok_keys.map(all_dfs.col(_)):_*), "source_id")

    val zoneHeight = 60/3600.0
    val object_table = matched_data.groupBy("obj_id", "zone")
                                   .agg(avg(matched_data.col("ALPHAWIN_J2000")),
                                        avg(matched_data.col("DELTAWIN_J2000")),
                                        avg(matched_data.col("MAG_BEST")))
                                   .withColumnRenamed("avg(DELTAWIN_J2000)", "dec")
                                   .withColumnRenamed("avg(ALPHAWIN_J2000)", "ra")
                                   .withColumnRenamed("avg(MAG_BEST)", "MAG_BEST")
                                   .filter(s"zone == ceil((dec + 90)/${zoneHeight})")
    object_table.printSchema

    val filtered_sources = object_table.select("obj_id").join(matched_data, "obj_id")

    object_table.write.parquet("object_table.parquet")
    filtered_sources.write.parquet("matched_data.parquet")

    System.exit(0)
  }

}
