package com.foursquare.slashem
import com.foursquare.elasticsearch.scorer.FourSquareScorePlugin
import com.foursquare.slashem._

import com.twitter.util.Duration

import org.bson.types.ObjectId
import org.junit.Test
import org.junit._

import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary.arbitrary

import org.specs.SpecsMatchers
import org.specs.matcher.ScalaCheckMatchers

//import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.node.Node
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory._;

import java.util.concurrent.TimeUnit
import java.util.UUID

import scalaj.collection.Imports._

import com.twitter.util.{Duration, ExecutorServiceFuturePool, Future, FuturePool, FutureTask, Throw, TimeoutException}
import java.util.concurrent.{Executors, ExecutorService}

object ElasticNode {
  val myUUID = UUID.randomUUID();
  val clusterName = "testcluster"+myUUID.toString()
  val node: Node = {
    nodeBuilder().local(true).client(false).clusterName(clusterName).node()
  }
}


class ElasticQueryTest extends SpecsMatchers with ScalaCheckMatchers {
  //This test exists because if futures get screwy
  //(as has happened) a number of the other tests will fail
  //spuriously and randomly (joy!).
  @Test
  def futureTest {
    val executor = Executors.newCachedThreadPool()
    val esfp = FuturePool(executor)
    val future : Future[Int]= esfp({
      Thread.sleep(100)
      1
    })
    Assert.assertEquals(1,future.apply(Duration(200,TimeUnit.MILLISECONDS)))
  }

  @Test(expected=classOf[TimeoutException])
  def futureTestBlock {
    val executor = Executors.newCachedThreadPool()
    val esfp = FuturePool(executor)
    var x = 1
    val future : Future[Int]= esfp({
      Thread.sleep(200)
      1
    })
    future.apply(Duration(10,TimeUnit.MILLISECONDS))
  }

  @Test(expected=classOf[TimeoutException])
  def testRecipGeoBoostTimeout {
    val geoLat = 74
    val geoLong = -31
    val r = ESimpleGeoPanda where (_.name contains "lolerskates") scoreBoostField(_.point recipSqeGeoDistance(geoLat, geoLong, 1, 5000, 1)) fetch(Duration(0,TimeUnit.MILLISECONDS))
  }

  @Test
  def testMatchAll {
    val r = ESimpleGeoPanda where (_.metall any) fetch()
    Assert.assertEquals(4,r.response.results.length)
  }


  @Test
  def simpleOrderTest {
    val fullQuery = ESimplePanda.where(_.name contains "lol")
                    .limit(5).orderDesc(_.followers)
    val r = fullQuery fetch()
  }

  @Test
  def simpleBoostTest {
    val fullQuery = ESimplePanda.where(_.name contains "lol")
                    .limit(5).boostField(_.followers)
    val r = fullQuery fetch()
  }


  @Test
  def testEmptySearch {
    try {
    val r = ESimplePanda where (_.name eqs "lolsdonotinsertsomethingwiththisinit") fetch()
    Assert.assertEquals(0,r.response.results.length)
    } catch {
      case e => e.printStackTrace()
    }
  }
  @Test
  def testNonEmptySearch {
    val r = ESimplePanda where (_.hobos contains "hobos") fetch()
    Assert.assertEquals(1,r.response.results.length)
    //Lets look at the document and make sure its what we expected
    val doc = r.response.results.apply(0)
    Assert.assertEquals("loler skates",doc.name.value)
    Assert.assertEquals("hobos",doc.hobos.value)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b11"),doc.id.is)
  }
  @Test
  def testNonEmptyMM100Search {
    val r = ESimplePanda where (_.name contains "loler eating hobo") minimumMatchPercent(100) fetch()
    Assert.assertEquals(1,r.response.results.length)
    //Lets look at the document and make sure its what we expected
    val doc = r.response.results.apply(0)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b18"),doc.id.is)
  }
  @Test
  def testNonEmptyMM100SearchWithTimeout {
    val r = ESimplePanda where (_.name contains "loler eating hobo") minimumMatchPercent(100) fetch(Duration(1, TimeUnit.SECONDS))
    Assert.assertEquals(1,r.response.results.length)
    //Lets look at the document and make sure its what we expected
    val doc = r.response.results.apply(0)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b18"),doc.id.is)
  }
  @Test
  def testNonEmptyMultiFieldSearch {
    val r = ESimplePanda.where(_.default contains "onlyinnamefield").queryField(_.name).queryField(_.hobos) fetch()
    Assert.assertEquals(1,r.response.results.length)
  }
  @Test
  def testNonEmptySearchOidScorePare {
    val r = ESimplePanda where (_.hobos contains "hobos") fetch()
    Assert.assertEquals(1,r.response.results.length)
    //Lets look at the document and make sure its what we expected
    val doc = r.response.oidScorePair.apply(0)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b11"),doc._1)
  }
  @Test
  def testSimpleInQuery {
    val r = ESimplePanda where (_.hobos in List("hobos")) fetch()
    Assert.assertEquals(1,r.response.results.length)
    //Lets look at the document and make sure its what we expected
    val doc = r.response.oidScorePair.apply(0)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b11"),doc._1)
  }
  @Test
  def testSimpleNInQuery {
    val r = ESimplePanda where (_.hobos nin List("hobos")) fetch()
    Assert.assertEquals(7,r.response.results.length)
  }
  @Test
  def testManyResultsSearch {
    val r = ESimplePanda where (_.name contains "loler") fetch()
    Assert.assertEquals(4,r.response.results.length)
  }
  @Test
  def testAndSearch {
    val r = ESimplePanda where (_.name contains "loler") and (_.hobos contains "nyet") fetch()
    Assert.assertEquals(1,r.response.results.length)
  }
  @Test
   def orderDesc {
    var r = ESimplePanda where (_.name contains "ordertest") orderDesc(_.followers) fetch()
    Assert.assertEquals(2,r.response.results.length)
    val doc0 = r.response.oidScorePair.apply(0)
    val doc1= r.response.oidScorePair.apply(1)
    Assert.assertEquals(doc0._1,new ObjectId("4c809f4251ada1cdc3790b14"))
    Assert.assertEquals(doc1._1,new ObjectId("4c809f4251ada1cdc3790b15"))
  }
  @Test
  def orderAsc {
    var r = ESimplePanda where (_.name contains "ordertest") orderAsc(_.followers) fetch()
    Assert.assertEquals(2,r.response.results.length)
    val doc0 = r.response.oidScorePair.apply(0)
    val doc1= r.response.oidScorePair.apply(1)
    Assert.assertEquals(doc0._1,new ObjectId("4c809f4251ada1cdc3790b15"))
    Assert.assertEquals(doc1._1,new ObjectId("4c809f4251ada1cdc3790b14"))
  }
  @Test
   def geoOrderDesc {
    var r = ESimpleGeoPanda where (_.name contains "ordertest") complexOrderDesc(_.point sqeGeoDistance(74.0,-31.0)) fetch()
    Assert.assertEquals(2,r.response.results.length)
    val doc0 = r.response.oidScorePair.apply(0)
    val doc1= r.response.oidScorePair.apply(1)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b16"),doc0._1)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b17"),doc1._1)
  }
  @Test
   def geoOrderAsc {
    var r = ESimpleGeoPanda where (_.name contains "ordertest") complexOrderAsc(_.point sqeGeoDistance(74.0,-31.0)) fetch()
    Assert.assertEquals(2,r.response.results.length)
    val doc0 = r.response.oidScorePair.apply(0)
    val doc1= r.response.oidScorePair.apply(1)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b17"),doc0._1)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b16"),doc1._1)
  }
  @Test
  def geoOrderIntAsc {
    var r = ESimpleGeoPanda where (_.name contains "ordertest") complexOrderAsc(_.point sqeGeoDistance(74,-31)) fetch()
    Assert.assertEquals(2,r.response.results.length)
    val doc0 = r.response.oidScorePair.apply(0)
    val doc1= r.response.oidScorePair.apply(1)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b17"),doc0._1)
    Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b16"),doc1._1)
  }


  @Test
  def testAndOrSearch {
    val r = ESimplePanda where (_.name contains "loler") and (x => (x.hobos contains "nyet") or (x.hobos contains "robot")) fetch()
    Assert.assertEquals(r.response.results.length,2)
  }
  @Test
  def testPhraseBoostOrdering {
    val rWithLowPhraseBoost = ESimplePanda where (_.name contains "loler skates") phraseBoost(_.name,10) fetch()
    val rWithHighPhraseBoost = ESimplePanda where (_.name contains "loler skates") phraseBoost(_.name,10000) fetch()
    val rNoPhraseBoost = ESimplePanda where (_.name contains "loler skates") fetch()
    Assert.assertEquals(4,rWithLowPhraseBoost.response.results.length)
    Assert.assertEquals(4,rWithHighPhraseBoost.response.results.length)
    Assert.assertEquals(4,rNoPhraseBoost.response.results.length)
    val doc1b = rWithLowPhraseBoost.response.results.apply(2)
    val doc2b = rWithHighPhraseBoost.response.results.apply(2)
    val doc3b = rNoPhraseBoost.response.results.apply(2)
    //Make sure the scores are actually impacted by the phraseBoost
    Assert.assertTrue(doc1b.score.value > doc2b.score.value)
    Assert.assertTrue(doc3b.score.value > doc1b.score.value)
    val lastResult = List(rWithLowPhraseBoost.response.results.apply(3),
                          rWithHighPhraseBoost.response.results.apply(3),
                          rNoPhraseBoost.response.results.apply(3)
                        )
    lastResult.map(doc => Assert.assertEquals(new ObjectId("4c809f4251ada1cdc3790b18"), doc.id.is))
  }
  @Test
  def testPhraseOnlyMatch {
    val phrase = "loler skates"
    val rPhraseOnly = ESimplePanda where (_.name eqs "loler skates") fetch()
    val rContains = ESimplePanda where (_.name contains "loler skates") fetch()
    rPhraseOnly.response.results.map(d => Assert.assertTrue(d.name.value.contains("loler skates")))
    val phraseCount = rPhraseOnly.response.results.length
    val containsCount =  rContains.response.results.length
    Assert.assertTrue(containsCount > phraseCount)
  }
  //@Test
  def testFieldFaceting {
    val r = ESimplePanda where (_.name contains "loler skates") facetField(_.foreign) fetch()
    Assert.assertEquals(4,r.response.results.length)
    Assert.assertEquals(1,r.response.fieldFacets.get("foreign").get("b"))
    Assert.assertEquals(3,r.response.fieldFacets.get("foreign").get("pants"))
  }

  @Test
  def testMaxCountFieldFaceting {
    val r = ESimplePanda where (_.name contains "loler skates") facetField(_.foreign) facetLimit(1) fetch()
    Assert.assertEquals(4,r.response.results.length)
    Assert.assertEquals(Some(None),r.response.fieldFacets.get("foreign").map(_.get("b")))
    Assert.assertEquals(3,r.response.fieldFacets.get("foreign").get("pants"))
  }


  @Test
  def testFieldBoost {
    val r1 = ESimplePanda where (_.magic contains "yes") fetch()
    val r2 = ESimplePanda where (_.magic contains "yes") boostField(_.followers,10) fetch()
    Assert.assertEquals(2,r1.response.results.length)
    Assert.assertEquals(2,r2.response.results.length)
    Assert.assertTrue(r2.response.results.apply(0).score.value > r1.response.results.apply(0).score.value)
  }

  @Test
  def testGeoBoost {
    //Test GeoBoosting. Note will actually make further away document come up first
    val geoLat = 74
    val geoLong = -31
    val r1 = ESimpleGeoPanda where (_.name contains "lolerskates") fetch()
    val r2 = ESimpleGeoPanda where (_.name contains "lolerskates") scoreBoostField(_.point sqeGeoDistance(geoLat, geoLong)) fetch()
    Assert.assertEquals(r1.response.results.length,2)
    Assert.assertEquals(r2.response.results.length,2)
    Assert.assertTrue(r2.response.results.apply(0).score.value > r1.response.results.apply(0).score.value)
  }
  @Test
  def testPointExtract {
    //Test GeoBoosting. Note will actually make further away document come up first
    val geoLat = 74
    val geoLong = -31
    val r = ESimpleGeoPanda where (_.name contains "lolerskates") scoreBoostField(_.point sqeGeoDistance(geoLat, geoLong)) fetch()
    Assert.assertEquals(r.response.results.length,2)
    Assert.assertEquals(r.response.results.apply(0).point.value._1,74.0,0.9)
  }

  @Test
  def testRecipGeoBoost {
    val geoLat = 74
    val geoLong = -31
    val r1 = ESimpleGeoPanda where (_.name contains "lolerskates") fetch()
    val r2 = ESimpleGeoPanda where (_.name contains "lolerskates") scoreBoostField(_.point recipSqeGeoDistance(geoLat, geoLong, 1, 5000, 1)) fetch()
    Assert.assertEquals(r1.response.results.length,2)
    Assert.assertEquals(r2.response.results.length,2)
    Assert.assertTrue(r2.response.results.apply(0).score.value > r1.response.results.apply(0).score.value)
  }

  @Test
  def testListFieldContains {
    val response1 = ESimplePanda where (_.favnums contains 2) fetch()
    val response2 = ESimplePanda where (_.favnums contains 6) fetch()
    val response3 = ESimplePanda where (_.nicknames contains "xzibit") fetch()
    val response4 = ESimplePanda where (_.nicknames contains "alvin") fetch()
    val response5 = ESimplePanda where (_.nicknames contains "dawg") fetch()
    val response6 = ESimplePanda where (_.favnums contains 9001) fetch()
    val response7 = ESimplePanda where (_.hugenums contains 1L) fetch()
    val response8 = ESimplePanda where (_.hugenums contains 9L) fetch()
    val response9 = ESimplePanda where (_.hugenums contains 9001L) fetch()
    val response10 = ESimplePanda where (_.favvenueids contains new ObjectId("4daf213893a0096fbaaef003")) fetch()
    Assert.assertEquals(response1.response.results.length, 2)
    Assert.assertEquals(response2.response.results.length, 1)
    Assert.assertEquals(response3.response.results.length, 2)
    Assert.assertEquals(response4.response.results.length, 2)
    Assert.assertEquals(response5.response.results.length, 1)
    Assert.assertEquals(response6.response.results.length, 0)
    Assert.assertEquals(response7.response.results.length, 2)
    Assert.assertEquals(response8.response.results.length, 1)
    Assert.assertEquals(response9.response.results.length, 0)
    Assert.assertEquals(response10.response.results.length, 1)
  }

  @Test
  def testListFieldIn {
    val response1 = ESimplePanda where (_.favnums in List(2, 3, 4, 5)) fetch()
    val response2 = ESimplePanda where (_.favnums in List(99)) fetch()
    val response3 = ESimplePanda where (_.termsfield in List("termhit", "lol")) fetch()
    val response4 = ESimplePanda where (_.favvenueids in List(new ObjectId("4daf213893a0096fbaaef003"))) fetch()
    Assert.assertEquals(response1.response.results.length, 2)
    Assert.assertEquals(response2.response.results.length, 0)
    Assert.assertEquals(response3.response.results.length, 1)
    Assert.assertEquals(response4.response.results.length, 1)
  }

  @Test
  def testIntListFieldEmptyIn {
    val response1 = ESimplePanda where (_.favnums in List()) fetch()
    val response2 = ESimplePanda where (_.termsfield in List()) fetch()
    Assert.assertEquals(response1.response.results.length, 0)
    Assert.assertEquals(response2.response.results.length, 0)
  }

 @Test
  def testIntListFieldEmptyNin {
    val response1 = ESimplePanda where (_.favnums nin List()) fetch()
    val response2 = ESimplePanda where (_.termsfield nin List()) fetch()
    Assert.assertEquals(response1.response.results.length, 8)
    Assert.assertEquals(response2.response.results.length, 8)
  }

  @Test
  def testObjectIdListFieldEmptyIn {
    val response1 = ESimplePanda where (_.favvenueids in List()) fetch()
    Assert.assertEquals(response1.response.results.length, 0)
  }

 @Test
  def testObjectIdListFieldEmptyNin {
    val response1 = ESimplePanda where (_.favvenueids nin List()) fetch()
    Assert.assertEquals(response1.response.results.length, 8)
  }  

  @Test
  def testListFieldNin {
    val idsWithFavNums = List(new ObjectId("4c809f4251ada1cdc3790b10"),
                               new ObjectId("4c809f4251ada1cdc3790b11"),
                               new ObjectId("4c809f4251ada1cdc3790b12"))

    val response1 = ESimplePanda where (_.favnums nin List(2, 3, 4, 5, 6)) fetch()
    val ids = response1.response.oids
    // No docs with ids should appear in the fetched docs.
    Assert.assertEquals(ids.intersect(idsWithFavNums).length, 0)

    val response2 = ESimplePanda where (_.favnums nin List(99)) fetch()
    val ids2 = response2.response.oids
    // All three docs with favnums should be returned, none contain 99
    Assert.assertEquals(ids2.intersect(idsWithFavNums).length, 3)

    val response3 = ESimplePanda where (_.termsfield nin List("termhit")) fetch()
    val ids3 = response3.response.oids
    // All three docs with favnums should be returned, none contain 99
    Assert.assertEquals(ids3.intersect(idsWithFavNums).length, 2)
  }

  @Test
  def testTermQueries {
    val res1 = ESimplePanda where (_.termsfield eqs "termhit") fetch()
    val res2 = ESimplePanda where (_.termsfield in List("randomterm", "termhit")) fetch()
    Assert.assertEquals(res1.response.results.length, 1)
    Assert.assertEquals(res2.response.results.length, 1)
  }

  @Test
  def testTermFilters {
    // grab 2 results, filter to 1
    val res1 = ESimplePanda where (_.hugenums contains 1L) filter(_.termsfield in List("termhit", "randomterm")) fetch()
    Assert.assertEquals(res1.response.results.length, 1)
  }

  @Test
  def testCustomScoreScripts {
    val params: Map[String, Any] = Map("lat" -> -31.1, "lon" -> 74.0, "weight" -> 2000, "weight2" -> 0.03)
    val response1 = ESimpleGeoPanda where(_.name contains "lolerskates") customScore("distance_score_magic", params) fetch()
    Assert.assertEquals(response1.response.results.length, 2)
  }

  @Before
  def hoboPrepIndex() {
    ESimplePanda.meta.node = ElasticNode.node
    ESimpleGeoPanda.meta.node = ElasticNode.node
    val plugin = new FourSquareScorePlugin()

    val client = ESimplePanda.meta.client

    //Set up the geo panda index
    val geoClient = ESimpleGeoPanda.meta.client
    try {
      val indexReq = Requests.createIndexRequest(ESimpleGeoPanda.meta.indexName)
      geoClient.admin.indices().create(indexReq).actionGet()
      geoClient.admin().indices().prepareRefresh().execute().actionGet()
      val geoPandaIndexName = ESimpleGeoPanda.meta.indexName
      val mapping = """
      { "slashemdoc" :{
        "properties" : {
          "point" : { type: "geo_point" }
        }
      }}"""
      val mappingReq = Requests.putMappingRequest(ESimpleGeoPanda.meta.indexName).source(mapping).`type`("slashemdoc")
      val mappingResponse = geoClient.admin().indices().putMapping(mappingReq).actionGet()
    } catch {
      case _ => println("Error creating geopanda stuff, may allready exist")
    }
    geoClient.admin().indices().prepareRefresh().execute().actionGet()
    val geodoc1 = geoClient.prepareIndex(ESimpleGeoPanda.meta.indexName,ESimpleGeoPanda.meta.docType,"4c809f4251ada1cdc3790b10").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","lolerskates")
                                                                          .field("point",74.0,-31.1)
                                                                          .field("id","4c809f4251ada1cdc3790b10")
                                                                          .field("decayedPopularity1", .5)
                                                                          .endObject()
      ).execute()
    .actionGet();
    val geodoc2 = geoClient.prepareIndex(ESimpleGeoPanda.meta.indexName,ESimpleGeoPanda.meta.docType,"4c809f4251ada1cdc3790b11").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","lolerskates")
                                                                          .field("id","4c809f4251ada1cdc3790b11")
                                                                          .field("point",74.0,-31.0)
                                                                          .field("decayedPopularity1", 21.2)
                                                                          .endObject()
      ).execute()
    .actionGet();


    //Set up the regular pandas
    val favnums1 = List(1, 2, 3, 4, 5).asJava
    val favnums2 = List(1, 2, 3, 4, 5).asJava
    val favnums3 = List(6, 7, 8, 9, 10).asJava
    val terms1 = List("termhit", "nohit").asJava
    val nicknames1 = List("jerry", "dawg", "xzibit").asJava
    val nicknames2 = List("xzibit", "alvin").asJava
    val nicknames3 = List("alvin", "nathaniel", "joiner").asJava
    val hugenums1 = List(1L, 2L, 3L).asJava
    val hugenums2 = List(1L, 9L, 8L).asJava
    val hugenums3 = List(7L, 10L, 13L).asJava
    val venueids1 = List("4daf213893a0096fbaaef003", "49ee02e9f964a52010681fe3", "42b21280f964a5206d251fe3").asJava
    val r = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b10").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","lolerskates")
                                                                          .field("id","4c809f4251ada1cdc3790b10")
                                                                          .field("favnums", favnums1)
                                                                          .field("nicknames", nicknames1)
                                                                          .field("hugenums", hugenums1)
                                                                          .field("termsfield", terms1)
                                                                          .field("favvenueids", venueids1)
                                                                          .endObject()
      ).execute()
    .actionGet();

    val r2 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b11").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","loler skates")
                                                                          .field("hobos","hobos")
                                                                          .field("id","4c809f4251ada1cdc3790b11")
                                                                          .field("foreign","pants")
                                                                          .field("favnums", favnums2)
                                                                          .field("nicknames", nicknames2)
                                                                          .field("hugenums", hugenums2)
                                                                          .endObject()
      ).execute()
    .actionGet();
    val r3 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b12").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","loler loler loler skates")
                                                                          .field("hobos","nyet")
                                                                          .field("followers",0)
                                                                          .field("magic","yes yes")
                                                                          .field("id","4c809f4251ada1cdc3790b12")
                                                                          .field("foreign","pants")
                                                                          .field("favnums", favnums3)
                                                                          .field("nicknames", nicknames3)
                                                                          .field("hugenums", hugenums3)
                                                                          .endObject()
      ).execute()
    .actionGet();
    val r4 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b13").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","loler loler loler loler loler loler loler loler chetos are delicious skates skates")
                                                                          .field("hobos","sounds like a robot is eating a")
                                                                          .field("followers",10)
                                                                          .field("magic","yes")
                                                                          .field("id","4c809f4251ada1cdc3790b13")
                                                                          .field("foreign","pants")
                                                                          .endObject()
      ).execute()
    .actionGet();
   val r5 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b18").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","loler eating a hobo")
                                                                          .field("followers",10)
                                                                          .field("id","4c809f4251ada1cdc3790b18")
                                                                          .field("foreign","b")
                                                                          .endObject()
      ).execute()
    .actionGet();
    val orderdoc1 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b14").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","ordertest")
                                                                          .field("followers",20)
                                                                          .field("id","4c809f4251ada1cdc3790b14")
                                                                          .endObject()
      ).execute()
    .actionGet();
    val orderdoc2 = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b15").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","ordertest")
                                                                          .field("followers",10)
                                                                          .field("id","4c809f4251ada1cdc3790b15")
                                                                          .endObject()
      ).execute()
    .actionGet();

    val geoOrderdoc1 = client.prepareIndex(ESimpleGeoPanda.meta.indexName,ESimpleGeoPanda.meta.docType,"4c809f4251ada1cdc3790b16").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","ordertest")
                                                                          .field("id","4c809f4251ada1cdc3790b16")
                                                                          .field("point",74.0,-32.0)
                                                                          .endObject()
      ).execute()
    .actionGet();
    val geoOrderdoc2 = geoClient.prepareIndex(ESimpleGeoPanda.meta.indexName,ESimpleGeoPanda.meta.docType,"4c809f4251ada1cdc3790b17").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","ordertest")
                                                                          .field("id","4c809f4251ada1cdc3790b17")
                                                                          .field("point",74.0,-31.0)
                                                                          .endObject()
      ).execute()
    .actionGet();
    val multifieldoc = client.prepareIndex(ESimplePanda.meta.indexName,ESimplePanda.meta.docType,"4c809f4251ada1cdc3790b19").setSource(jsonBuilder()
                                                                          .startObject()
                                                                          .field("name","ilikecheetos onlyinnamefield allright")
                                                                          .field("followers",10)
                                                                          .field("id","4c809f4251ada1cdc3790b19")
                                                                          .endObject()
      ).execute()
    .actionGet();


    client.admin().indices().prepareRefresh().execute().actionGet()
    geoClient.admin().indices().prepareRefresh().execute().actionGet()

  }
  @After
  def hoboDone() {
    ESimplePanda.meta.node = ElasticNode.node
    ESimpleGeoPanda.meta.node = ElasticNode.node
    try {
      val client = ESimplePanda.meta.client
      val geoClient = ESimpleGeoPanda.meta.client
      val geoIndexDelReq = Requests.deleteIndexRequest(ESimpleGeoPanda.meta.indexName)
      val indexDelReq = Requests.deleteIndexRequest(ESimplePanda.meta.indexName)
      geoClient.admin.indices().delete(geoIndexDelReq)
      client.admin.indices().delete(indexDelReq)
    } catch {
      case _ => println("Error cleaning up after tests... oh well")
    }
  }

}
