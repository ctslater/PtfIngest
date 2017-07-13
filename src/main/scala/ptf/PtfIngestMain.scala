
package ptf

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.io.File
import PtfIngest._

/*
 * Program Entry
 */

object PtfIngestMain {

  val spark = SparkSession.builder().appName("TestSparkSql").getOrCreate()
  import spark.implicits._

  def df_with_source_id(ptfFile: PtfFilename): DataFrame = {
    import org.apache.spark.sql.functions.lit
    val df = spark.read.parquet(ptfFile.filename)
    df.withColumn("source_id", df("number") + 10000*ptfFile.serial.toLong).withColumn("visit", lit(ptfFile.date.toLong))
  }

  def main(args: Array[String]) {

    val data_dir = new File(args(0))

    if(data_dir.listFiles == null) {
      println(s"Data directory ${args(0)} is invalid")
      System.exit(1)
    }

    val ptf_files = data_dir.listFiles.map(_.toString).map(PtfFilename(_)).flatten

    val unique_dates = ptf_files.map(_.date).distinct
    val exposure1_files = ptf_files.filter(_.date == unique_dates(0))
    val exposure1_df = exposure1_files.map(df_with_source_id).reduceRight(_.union(_))

    val exposure2_files = ptf_files.filter(_.date == unique_dates(1))
    val exposure2_df = exposure2_files.map(df_with_source_id).reduceRight(_.union(_))

    val zone_df = exposure1_df.union(exposure2_df).as[CoordsId].flatMap(assign_zones(_))
    val matches = zone_df.groupByKey(_.zone).flatMapGroups(match_within_zone).limit(10).show()
    System.exit(0)
  }

}
