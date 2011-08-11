package com.foursquare.solr
import Ast._
import net.liftweb.util.Props
import net.liftweb.common.Box

object SVenue extends SVenue with SolrMeta[SVenue] {
  def solrName = "venues"
  def host = Props.get("solr." + solrName + ".host").openOr(throw new RuntimeException("Host Props not found for %s Solr".format(solrName)))
  def port = Props.getInt("solr." + solrName + ".port").openOr(throw new RuntimeException("Port Props not found for %s Solr".format(solrName))).toString
  def servers = Props.get("sorl."+name+".servers").map(x => x.split(",").toList).openOr {
    val Host = Props.get("solr." + name + ".host").openOr(throw new RuntimeException("Props not found for %s Solr".format(name)))
    val Port = Props.getInt("solr." + name + ".port").openOr(throw new RuntimeException("Props not found for %s Solr".format(name)))
    List("%s:%d".format(Host, Port))
  }
}
class SVenue extends SolrSchema[SVenue] {
  def meta = SVenue

  object default extends SolrDefaultStringField(this)
  object id extends SolrObjectIdField(this)
  object lat extends SolrDoubleField(this)
  object lng extends SolrDoubleField(this)
  object userid extends SolrLongField(this)
  object text extends SolrStringField(this)
  object name extends SolrStringField(this)
  object ngram_name extends SolrStringField(this)
  object keywords extends SolrStringField(this)
  object aliases extends SolrStringField(this)
  object tags extends SolrStringField(this)
  object category_string extends SolrStringField(this)
  object address extends SolrStringField(this)
  object score extends SolrDoubleField(this)
  object mayorid extends SolrLongField(this)
  object category_id_0 extends SolrObjectIdField(this)
  object popularity extends SolrIntField(this)
  object dtzone extends SolrStringField(this)
  object partitionedPopularity extends SolrIntListField(this)
  object geomobile extends SolrBooleanField(this)
  object checkin_info extends SolrStringField(this)
  object hasSpecial extends SolrBooleanField(this)
  object crossstreet extends SolrStringField(this)
  object city extends SolrStringField(this)
  object state extends SolrStringField(this)
  object zip extends SolrStringField(this)
  object country extends SolrStringField(this)
  object checkinCount extends SolrIntField(this)
  object category_ids extends SolrStringField(this)
  object decayedPopularity1 extends SolrDoubleField(this)
}

object STip extends STip with SolrMeta[STip] {
  def host = "repo.foursquare.com"
  def port = "8000"
  def servers = List(host+":"+port)
}
class STip extends SolrSchema[STip] {
  def meta = STip

  object id extends SolrObjectIdField(this)
  object text extends SolrStringField(this)
  object userid extends SolrStringField(this)
}

object SUser extends SUser with SolrMeta[SUser] {
  def host = "repo.foursquare.com"
  def port = "8001"
  def servers = List(host+":"+port)
}
class SUser extends SolrSchema[SUser] {
  def meta = SUser

  object id extends SolrObjectIdField(this)
  object fullname extends SolrStringField(this)
  object firstname extends SolrStringField(this)
  object lastname extends SolrStringField(this)
  object email extends SolrStringField(this)
  object phone extends SolrStringField(this)
  object user_type extends SolrStringField(this)
}


object Helpers {
  def groupWithOr[V](v: Iterable[Query[V]]): Query[V] = {
    Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => Or(l, r)}))
  }

  def groupWithAnd[V](v: Iterable[Query[V]]): Query[V] = {
    Group(v.tail.foldLeft(v.head: Query[V])({(l, r) => And(l, r)}))
  }
}
