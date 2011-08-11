package com.foursquare.solr
import com.foursquare.solr.Ast._
import com.foursquare.lib.GeoS2
import net.liftweb.record.{Record}

// Phantom types
abstract sealed class Ordered
abstract sealed class Unordered
abstract sealed class Limited
abstract sealed class Unlimited
trait minimumMatchType
abstract sealed class defaultMM extends minimumMatchType
abstract sealed class customMM extends minimumMatchType

case class QueryBuilder[M <: Record[M], Ord, Lim, MM <: minimumMatchType](
 meta: M with SolrSchema[M],
 clauses: List[Clause[_]],  // Like AndCondition in MongoHelpers
 filters: List[Clause[_]],
 boostQueries: List[Clause[_]],
 queryFields: List[WeightedField],
 phraseBoostFields: List[PhraseWeightedField],
 start: Option[Long],
 limit: Option[Long],
 sort: Option[String],
 minimumMatch: Option[String],
 queryType: Option[String],
 fieldsToFetch: List[String]) {

  val GeoS2FieldName = "geo_s2_cell_ids"
  val DefaultLimit = 10
  val DefaultStart = 0
  import Helpers._

  def and[F](c: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM] = {
    QueryBuilder(meta, List(c(meta)), filters=filters, boostQueries=boostQueries, queryFields=queryFields, phraseBoostFields=phraseBoostFields, start=start, limit=limit, sort=sort, queryType=queryType, minimumMatch=minimumMatch,fieldsToFetch=fieldsToFetch)
  }

  def filter[F](f: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM] = {
    QueryBuilder(meta, clauses, f(meta) :: filters, boostQueries, queryFields, phraseBoostFields, start, limit, sort, minimumMatch, queryType, fieldsToFetch)
  }

  def boostQuery[F](f: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM] = {
    QueryBuilder(meta, clauses, filters, f(meta)::boostQueries, queryFields, phraseBoostFields, start, limit, sort, minimumMatch, queryType, fieldsToFetch)
  }

  def limit(l: Int)(implicit ev: Lim =:= Unlimited): QueryBuilder[M, Ord, Limited, MM] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, start, Some(l), sort=None, minimumMatch, queryType, fieldsToFetch)
  }

  def orderAsc[F](f: M => SolrField[F, M])(implicit ev: Ord =:= Unordered): QueryBuilder[M, Ordered, Lim, MM] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, start, limit, sort=Some(f(meta).name + " asc"), minimumMatch, queryType, fieldsToFetch)
  }

  def orderDesc[F](f: M => SolrField[F, M])(implicit ev: Ord =:= Unordered): QueryBuilder[M, Ordered, Lim, MM] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, start, limit, sort=Some(f(meta).name + "desc"), minimumMatch, queryType, fieldsToFetch)
  }

  def geoRadiusFilter(geoLat: Double, geoLong: Double, radiusInMeters: Int, maxCells: Int = GeoS2.DefaultMaxCells): QueryBuilder[M, Ord, Lim, MM] = {
    val cellIds = GeoS2.cover(geoLat, geoLong, radiusInMeters, maxCells=maxCells).map({x: com.google.common.geometry.S2CellId => Phrase(x.toToken)})
    val geoFilter = Clause(GeoS2FieldName, groupWithOr(cellIds))
    QueryBuilder(meta, clauses, geoFilter :: filters, boostQueries, queryFields, phraseBoostFields, start, limit, sort, minimumMatch, queryType, fieldsToFetch)
  }

  def minimumMatchPercent(percent: Int)(implicit ev: MM =:= defaultMM) : QueryBuilder[M, Ord, Lim, customMM] = {
     QueryBuilder(meta,clauses, filters, boostQueries, queryFields, phraseBoostFields, start,limit, sort, Some(percent.toString+"%"), queryType, fieldsToFetch)
  }
  def minimumMatchAbsolute(count: Int)(implicit ev: MM =:= defaultMM) : QueryBuilder[M, Ord, Lim, customMM] = {
     QueryBuilder(meta,clauses, filters, boostQueries, queryFields, phraseBoostFields, start,limit, sort, Some(count.toString), queryType, fieldsToFetch)
  }
  def useQueryType(qt : String) : QueryBuilder[M, Ord, Lim, MM] ={
     QueryBuilder(meta,clauses, filters, boostQueries, queryFields, phraseBoostFields, start,limit, sort, minimumMatch, Some(qt), fieldsToFetch)
  }

  def queryField[F](f : M => SolrField[F,M], boost: Double = 1): QueryBuilder[M, Ord, Lim, MM] ={
     QueryBuilder(meta,clauses, filters, boostQueries, WeightedField(f(meta).name,boost)::queryFields, phraseBoostFields, start,limit, sort, minimumMatch, queryType, fieldsToFetch)
  }

  def phraseBoost[F](f : M => SolrField[F,M], boost: Double = 1, pf: Boolean = true, pf2: Boolean = true, pf3: Boolean = true): QueryBuilder[M, Ord, Lim, MM] ={
     QueryBuilder(meta,clauses, filters, boostQueries, queryFields, PhraseWeightedField(f(meta).name,boost,pf,pf2,pf3)::phraseBoostFields, start,limit, sort, minimumMatch, queryType, fieldsToFetch)
  }

  def fetchField[F](f : M => SolrField[F,M]) = {
    QueryBuilder(meta,clauses, filters, boostQueries, queryFields, phraseBoostFields, start,limit, sort, minimumMatch, queryType, f(meta).name::fieldsToFetch)
  }

  def test(): Unit = {
    val q = clauses.map(_.extend).mkString
    println("clauses: " + clauses.map(_.extend).mkString)
    println("filters: " + filters.map(_.extend).mkString)
    println("start: " + start)
    println("limit: " + limit)
    println("sort: " + sort)
    println(queryParams)
    ()
  }

  def queryParams(): Seq[(String,String)] = {
    val p = List(("q" -> clauses.map(_.extend).mkString(" ")),
                 ("start" -> (start.getOrElse {DefaultStart}).toString),
                 ("rows" -> (limit.getOrElse {DefaultLimit}).toString)
                 )

    val s = sort match {
      case None => Nil
      case Some(sort) => List("sort" -> sort)
    }
    val qt = queryType match {
      case None => Nil
      case Some(method) => List("defType" -> method)
    }
    val mm = minimumMatch match {
      case None => Nil
      case Some(mmParam) => List("mm" -> mmParam)
    }

    val bq = boostQueries.map({ x => ("bq" -> x.extend)})

    val qf = queryFields.filter({x => x.boost != 0}).map({x => ("qf" -> x.extend)})

    val pf = phraseBoostFields.filter(x => x.pf).map({x => ("pf" -> x.extend)})++phraseBoostFields.filter(x => x.pf2).map({x => ("pf2" -> x.extend)})++
             phraseBoostFields.filter(x => x.pf3).map({x => ("pf3" -> x.extend)})

    val fl = fieldsToFetch match {
      case Nil => Nil
      case x => List(("fl" -> (x.mkString(","))))
    }

    val f = filters.map({x => ("fq" -> x.extend)})

     mm ++ qt ++ bq ++ qf ++ p ++ s ++ f ++ pf ++ fl
  }

  def fetch(l: Int)(implicit ev: Lim =:= Unlimited): SearchResults[M] = {
    this.limit(l).fetch
  }
  def fetch():  SearchResults[M] = {
    // Gross++
    meta.query(queryParams,fieldsToFetch)
  }
}
