package com.foursquare.slashem
import com.foursquare.slashem.Ast._
import net.liftweb.record.{Record}

// Phantom types
abstract sealed class Ordered
abstract sealed class Unordered
abstract sealed class Limited
abstract sealed class Unlimited
trait MinimumMatchType
abstract sealed class defaultMM extends MinimumMatchType
abstract sealed class customMM extends MinimumMatchType

case class QueryBuilder[M <: Record[M], Ord, Lim, MM <: MinimumMatchType](
 meta: M with SolrSchema[M],
 clauses: AClause,  // Like AndCondition in MongoHelpers
 filters: List[AClause],
 boostQueries: List[AClause],
 queryFields: List[WeightedField],
 phraseBoostFields: List[PhraseWeightedField],
 boostFields: List[String],
 start: Option[Long],
 limit: Option[Long],
 tieBreaker: Option[Double],
 sort: Option[String],
 minimumMatch: Option[String],
 queryType: Option[String],
 fieldsToFetch: List[String]) {


  val DefaultLimit = 10
  val DefaultStart = 0
  import Helpers._

  def and[F](c: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(meta=meta,clauses=JoinClause(c(meta),clauses,"AND"))
  }

  def or[F](c: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(meta=meta,clauses=JoinClause(c(meta),clauses,"OR"))
  }


  //Filter the result set. Filter queries can be run in parallel from the main query and
  //have a separate cache. Filter queries are great for queries that are repeated often which
  //you want to constrain your result set by.
  def filter[F](f: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(filters=f(meta)::filters)
  }

  //A boostQuery affects the scoring of the results.
  def boostQuery[F](f: M => Clause[F]): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(boostQueries=f(meta) :: boostQueries)
  }

  //Where you want to start fetching results back from
  def start(s: Int): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(start=Some(s))
  }

  //Only fetch back l results
  def limit(l: Int)(implicit ev: Lim =:= Unlimited): QueryBuilder[M, Ord, Limited, MM] = {
    this.copy(limit=Some(l))
  }

  //In edismax the score is max({scores})+tieBreak*\sum{scores})
  def tieBreaker(t: Double): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(tieBreaker=Some(t))
  }

  //Right now we only support ordering by field
  //TODO: Support ordering by function query
  def orderAsc[F](f: M => SolrField[F, M])(implicit ev: Ord =:= Unordered): QueryBuilder[M, Ordered, Lim, MM] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, boostFields, start, limit, tieBreaker, sort=Some(f(meta).name + " asc"), minimumMatch, queryType, fieldsToFetch)
  }

  def orderDesc[F](f: M => SolrField[F, M])(implicit ev: Ord =:= Unordered): QueryBuilder[M, Ordered, Lim, MM] = {
    QueryBuilder(meta, clauses, filters, boostQueries, queryFields, phraseBoostFields, boostFields, start, limit, tieBreaker, sort=Some(f(meta).name + " desc"), minimumMatch, queryType, fieldsToFetch)
  }

  //If you doing a phrase search this the percent of terms that must match, rounded down
  //So if you have it set to 50 and then do a search with 3 terms at least one term must match
  //A search of 4 however would require 2 terms to match.
  def minimumMatchPercent(percent: Int)(implicit ev: MM =:= defaultMM) : QueryBuilder[M, Ord, Lim, customMM] = {
    this.copy(minimumMatch=Some(percent.toString+"%"))
  }

  //This is an absolute # of terms rather than a percent of the query terms to match
  //Note: You must chose one or the other.
  def minimumMatchAbsolute(count: Int)(implicit ev: MM =:= defaultMM) : QueryBuilder[M, Ord, Lim, customMM] = {
    this.copy(minimumMatch=Some(count.toString))
  }
  //Set the query type. This corresponds to the "defType" field. Some sample values include "edismax" , "dismax"
  //or just empty to use the default query type
  def useQueryType(qt : String) : QueryBuilder[M, Ord, Lim, MM] ={
    this.copy(queryType=Some(qt))
  }

  //Depending on the query type you set, you can specify different fields to be queried.
  //This allows you to set a field and a boost.
  //Fair warning: If you set this value, it may be ignored (it is by the default solr query parser)
  def queryField[F](f : M => SolrField[F,M], boost: Double = 1): QueryBuilder[M, Ord, Lim, MM] ={
    this.copy(queryFields=WeightedField(f(meta).name,boost)::queryFields)
  }

  //Same as above but takes a list of fields.
  def queryFields(fs : List[M => SolrField[_,M]], boost: Double = 1): QueryBuilder[M, Ord, Lim, MM] ={
    this.copy(queryFields=fs.map(f => WeightedField(f(meta).name,boost))++queryFields)
  }

  //Certain query parsers allow you to set a phraseBoost field. Generally these are only run on the returned
  //documents. So if I want to return all documents matching either coffee or shop but I want documents
  //with "coffee shop" to score higher I would set this.
  //The params for pf,pf2,and pf3 control what type of phrase boost query to generate. In edismax pf2/pf3 results
  //in a query which will match shingled phrase queries of length 2 & 3 respectively. For example pf2=true in edismax and
  //a query of "delicious coffee shops" would boost documents containing "delicious coffee" and "coffee shops".
  def phraseBoost[F](f : M => SolrField[F,M], boost: Double = 1, pf: Boolean = true, pf2: Boolean = true, pf3: Boolean = true): QueryBuilder[M, Ord, Lim, MM] ={
    this.copy(phraseBoostFields=PhraseWeightedField(f(meta).name,boost,pf,pf2,pf3)::phraseBoostFields)
  }

  //Specify a field to be retrieved. If you want to get back all fields you can use a field of name "*"
  def fetchField[F](f : M => SolrField[F,M]): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(fieldsToFetch=f(meta).name::fieldsToFetch)
  }

  //Same as above but takes multiple fields
  def fetchFields(fs : (M => SolrField[_,M])*): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(fieldsToFetch=fs.map(f=> f(meta).name).toList++fieldsToFetch)
  }

  def boostField(s: String): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(boostFields=s::boostFields)
  }

  def boostField[F](f : M => SolrField[F,M], boost: Double = 1): QueryBuilder[M, Ord, Lim, MM] = {
    this.copy(boostFields=(f(meta).name+"^"+boost)::boostFields)
  }

  //Print out some debugging information.
  def test(): Unit = {
    println("clauses: " + clauses.extend)
    println("filters: " + filters.map(_.extend).mkString)
    println("start: " + start)
    println("limit: " + limit)
    println("sort: " + sort)
    println(queryParams)
    ()
  }

  def queryParams(): Seq[(String, String)] = queryParamsWithBounds(this.start, this.limit)

  def queryParamsWithBounds(qstart: Option[Long], qrows: Option[Long]): Seq[(String,String)] = {
    val bounds = List(("start" -> (qstart.getOrElse {DefaultStart}).toString),
                 ("rows" -> (qrows.getOrElse {DefaultLimit}).toString))
    bounds ++ queryParamsNoBounds()
  }

  //This is the part which generates most of the solr request
  def queryParamsNoBounds(): Seq[(String,String)] = {
    val p = List(("q" -> clauses.extend))

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
      case x => List("fl" -> (x.mkString(",")))
    }

    val t = tieBreaker match {
      case None => Nil
      case Some(x) => List("tieBreaker" -> x.toString)
    }

    val bf = boostFields.map({x => ("bf" -> x)})

    val f = filters.map({x => ("fq" -> x.extend)})

     t ++ mm ++ qt ++ bq ++ qf ++ p ++ s ++ f ++ pf ++ fl ++ bf
  }

  //Fetch the results with the limit of l
  def fetch(l: Int)(implicit ev: Lim =:= Unlimited): SearchResults[M] = {
    this.limit(l).fetch
  }

  //fetch the results
  def fetch():  SearchResults[M] = {
    // Gross++
    meta.query(queryParams,fieldsToFetch)
  }
  // Call fetchBatch when you need a large number of results from SOLR.
  // Usage example: val res = (SVenue where (_.default eqs "coffee") start(10) limit(40) fetchBatch(10)) {_.response.oids }
  def fetchBatch[T](batchSize: Int)(f: SearchResults[M] => List[T]): List[T] = {
    val startPos: Long = this.start.getOrElse(DefaultStart)
    val maxRowsToGet: Option[Long] = this.limit//If not specified try to get all rows
    //There is somewhat of a race condition here. If data is being inserted or deleted during the query
    //some results may not appear and some results may be duplicated.
    val firstQuery = meta.query(queryParamsWithBounds(Option(startPos), Option(batchSize)), fieldsToFetch)
    val maxResults = firstQuery.response.numFound - firstQuery.response.start
    val rowsToGet : Long = maxRowsToGet.map(scala.math.min(_,maxResults)) getOrElse maxResults
    // Now make rowsToGet/batchSizes calls to meta.query
    //Note the 1 is not a typo since we have already fetched the first page.
    f(firstQuery)++(1 to scala.math.ceil(rowsToGet*1.0/batchSize).toInt).flatMap{i =>
      // cannot simply override this.start as it is a val, so removing/adding on queryParams
      val starti = startPos + (i*batchSize)
      f(meta.query(queryParamsWithBounds(Option(starti), Option(batchSize)), fieldsToFetch))
    }.toList
  }
}

object Helpers {
  def groupWithOr[V](v: Iterable[Query[V]]): Query[V] = {
    if (v.isEmpty)
      Group(Empty[V])
    else
      Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => Or(l, r)}))
  }

  def groupWithAnd[V](v: Iterable[Query[V]]): Query[V] = {
    if (v.isEmpty)
      Group(Empty[V])
    else
      Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => And(l, r)}))
  }
}
