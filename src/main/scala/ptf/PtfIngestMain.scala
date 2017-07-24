
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
    /* This function is no longer used since we rely on Spark's globbing
       of S3 paths. Saving in case it's useful later though.
    */
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
    } while (moreResults)
    keys.toList
  }

  def main(args: Array[String]) {

    val useS3 = true

    val ok_keys = Seq("source_id", "visit", "ALPHAWIN_J2000", "DELTAWIN_J2000",
      "MAG_BEST", "MAGERR_BEST",
      "FLUX_AUTO", "FLUXERR_AUTO",
      "FLAGS", "CLASS_STAR", "ZEROPOINT")

    val allDFs = spark.read
                      .option("mergeSchema", "false")
                      .parquet("s3://palomar-transient-factory/input_parquet/*parquet")

    val zone_df = allDFs.as[CoordsId].flatMap(assign_zones(_))
    val matches = zone_df.groupByKey(_.zone).flatMapGroups(match_within_zone)

    val matched_data = matches.join(allDFs.select(cols=ok_keys.map(allDFs.col(_)):_*), "source_id")

    // This should share zone determination code with assign_zones, but for
    // the moment this duplication is workable.
    val zoneHeight = 5/3600.0
    val object_table = matched_data.groupBy("obj_id", "zone")
                                   .agg(avg(matched_data.col("ALPHAWIN_J2000")),
                                        avg(matched_data.col("DELTAWIN_J2000")),
                                        avg(matched_data.col("MAG_BEST")))
                                   .withColumnRenamed("avg(DELTAWIN_J2000)", "dec")
                                   .withColumnRenamed("avg(ALPHAWIN_J2000)", "ra")
                                   .withColumnRenamed("avg(MAG_BEST)", "MAG_BEST")
                                   .filter(s"zone == ceil((dec + 90)/${zoneHeight})")

    val filtered_sources = object_table.select("obj_id").join(matched_data, "obj_id")

    if (useS3) {
      object_table.write.parquet("s3://palomar-transient-factory/object_table.parquet")
      filtered_sources.write.parquet("s3://palomar-transient-factory/matched_data.parquet")
    } else {
      object_table.write.parquet("object_table.parquet")
      filtered_sources.write.parquet("matched_data.parquet")
    }

    System.exit(0)
  }

}
