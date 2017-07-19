
package ptf

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.functions.avg
import java.io.File

import scala.collection.JavaConversions._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}

import PtfIngest._

object PtfIngestMain {

  val spark = SparkSession.builder().appName("PtfIngest").getOrCreate()
  import spark.implicits._

  def df_with_source_id(ptfFile: PtfFilename): DataFrame = {
    import org.apache.spark.sql.functions.lit
    val df = spark.read.parquet(ptfFile.filename)
    df.withColumn("source_id", df("number") + 10000*ptfFile.serial.toLong)
      .withColumn("visit", lit(ptfFile.date.toLong))
  }

  def getKeysFromS3(bucketName: String, prefix: String): List[String] = {
    val s3client: AmazonS3 = new AmazonS3Client(new InstanceProfileCredentialsProvider())

    var req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix)
    val keys =  ListBuffer[String]()

    // This loop is gross
    var moreResults = false
    do {
      val result = s3client.listObjectsV2(req)
      keys ++= result.getObjectSummaries.toList.map(_.getKey())
      req.setContinuationToken(result.getNextContinuationToken())
      moreResults = result.isTruncated()
      print("$")

    } while (moreResults)
    keys.toList
  }

  def main(args: Array[String]) {

    val use_s3 = true

    /*
    val ptfFiles: Seq[PtfFilename] = if (use_s3) {
      val s3Keys = getKeysFromS3("palomar-transient-factory", "input_parquet")
      s3Keys.map(key => s"s3://palomar-transient-factory/${key}").map(PtfFilename(_)).flatten
    } else {
      val data_dir = new File(args(0))
      if(data_dir.listFiles == null) {
        println(s"Data directory ${args(0)} is invalid")
        System.exit(1)
      }
      data_dir.listFiles.toList.map(_.toString).map(PtfFilename(_)).flatten
    }


    println("------")
    println("Parquet keys retreived")
    println("------")
    */

    val ok_keys = Seq("source_id", "visit", "ALPHAWIN_J2000", "DELTAWIN_J2000",
      "MAG_BEST", "MAGERR_BEST",
      "FLUX_AUTO", "FLUXERR_AUTO",
      "FLAGS", "CLASS_STAR", "ZEROPOINT")

    val small_subset = false

    /*
    val all_dfs = if (small_subset) {
      val unique_dates = ptfFiles.map(_.date).distinct
      val exposure1_files = ptfFiles.filter(_.date == unique_dates(0))
      val exposure1_df = exposure1_files.map(df_with_source_id).reduceRight(_.union(_))

      val exposure2_files = ptfFiles.filter(_.date == unique_dates(1))
      val exposure2_df = exposure2_files.map(df_with_source_id).reduceRight(_.union(_))
      exposure1_df.union(exposure2_df)
    } else
      ptfFiles.map(df_with_source_id).reduceRight(_.union(_)).coalesce(30)
    */

    val allDFs = spark.read
                      .option("mergeSchema", "false")
                      .parquet("s3://palomar-transient-factory/input_parquet/*parquet")

    val zone_df = allDFs.as[CoordsId].flatMap(assign_zones(_))
    val matches = zone_df.groupByKey(_.zone).flatMapGroups(match_within_zone)

    val matched_data = matches.join(allDFs.select(cols=ok_keys.map(allDFs.col(_)):_*), "source_id")

    // This should share zone determination code with assign_zones, but for
    // the moment this duplication is workable.
    val zoneHeight = 30/3600.0
    val object_table = matched_data.groupBy("obj_id", "zone")
                                   .agg(avg(matched_data.col("ALPHAWIN_J2000")),
                                        avg(matched_data.col("DELTAWIN_J2000")),
                                        avg(matched_data.col("MAG_BEST")))
                                   .withColumnRenamed("avg(DELTAWIN_J2000)", "dec")
                                   .withColumnRenamed("avg(ALPHAWIN_J2000)", "ra")
                                   .withColumnRenamed("avg(MAG_BEST)", "MAG_BEST")
                                   .filter(s"zone == ceil((dec + 90)/${zoneHeight})")

    val filtered_sources = object_table.select("obj_id").join(matched_data, "obj_id")

    if (use_s3) {
      object_table.write.parquet("s3://palomar-transient-factory/object_table.parquet")
      filtered_sources.write.parquet("s3://palomar-transient-factory/matched_data.parquet")
    } else {
      object_table.write.parquet("object_table.parquet")
      filtered_sources.write.parquet("matched_data.parquet")

    }

    System.exit(0)
  }

}
