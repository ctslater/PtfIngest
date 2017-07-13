
package ptf

import org.scalatest.{FlatSpec, OptionValues, Matchers}
import PtfIngest._
import ObjectSet.{RaDec, SourceId, ObjectId}

abstract class UnitSpec extends FlatSpec with OptionValues with Matchers

class PtfTest extends UnitSpec {
  "PtfFilename" should "correctly parse filenames" in {
    val filenameString = "PTF_200912024227_c_p_scie_t100845_u011771826_f02_p101001_c09.parquet"
    val parsedFilename = PtfFilename(filenameString)
    parsedFilename.value.date shouldBe "200912024227"
    parsedFilename.value.timestamp shouldBe "100845"
    parsedFilename.value.serial shouldBe "011771826"
    parsedFilename.value.chip shouldBe "09"
  }

  it should "reject bad filenames" in {
    val badFilenameString = "PTF_200912024227_c_p_scie_XXX100845_u011771826_f02_p101001_c09.parquet"
    val parsedFilename = PtfFilename(badFilenameString)
    parsedFilename shouldBe None
  }

}

class ObjectSetTest extends UnitSpec {
  "ObjectSet" should "start empty" in {
    val objects = new ObjectSet
    objects.objectTree.size shouldBe 0
    objects.sourceToObject.size shouldBe 0
  }

  it should "accept a first set of sources" in {
    val sources: Seq[(RaDec, SourceId)] =
      Seq( ((1.1, 1.2), 1),
           ((2.1, 2.2), 2),
           ((3.1, 3.2), 3),
           ((4.1, 4.2), 4))
    val objects = new ObjectSet
    objects.addNewSources(sources)

    objects.objectTree.size shouldBe 4
    objects.sourceToObject.size shouldBe 4
  }

}
