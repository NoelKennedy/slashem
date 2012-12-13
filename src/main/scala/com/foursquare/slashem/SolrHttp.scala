package com.foursquare.slashem

import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffers


sealed abstract class HttpVerb
case object HttpPost extends HttpVerb
case object HttpGet extends HttpVerb

trait SolrHttp {

  def queryPath:String

  def logger: SolrQueryLogger


  def queryString(params: Seq[(String, String)]): QueryStringEncoder = {
    val qse = new QueryStringEncoder(queryPath)
    qse.addParam("wt", "json")
    params.foreach( x => {
      qse.addParam(x._1, x._2)
    })
    qse
  }

  def makeHttpGetRequest(params: Seq[(String, String)]): HttpRequest = {
    // Ugly
    val qse = queryString(params ++
      logger.queryIdToken.map("magicLoggingToken" -> _).toList)

    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, qse.toString)
    request
  }

  def makeHttpPostRequest(params: Seq[(String, String)]):HttpRequest = {
    val newParams = logger.queryIdToken.map("magicLoggingToken" -> _) ++ (("wt", "json") +: params)
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, queryPath)
    val bodyBuilder = newParams.foldLeft(new StringBuilder())((sb, kv) => {
      val (key, value) = kv
      sb.append(key)
        .append("=")
        .append(value)
        .append("&")
    })
    bodyBuilder.deleteCharAt(bodyBuilder.length - 1)

    val channel = ChannelBuffers.copiedBuffer(bodyBuilder, CharsetUtil.UTF_8)
    request.setContent(channel)

    import HttpHeaders.Names._

    //request.addHeader(HOST, servers.head)
    request.addHeader(CONTENT_TYPE, "application/x-www-form-urlencoded")
    request.setHeader(CONNECTION, "keep-alive")
    request.setHeader(CONTENT_LENGTH, channel.readableBytes())
    request

//    val loggedClient = logFilter andThen client
//    (loggedClient(request)).map(response => {
//      response.getStatus match {
//        case HttpResponseStatus.OK => response.getContent.toString(CharsetUtil.UTF_8)
//        case status => throw SolrResponseException(status.getCode, status.getReasonPhrase, solrName, queryPath)
//      }
//    })
  }

  // This method performs the actually query / http request. It should probably
  // go in another file when it gets more sophisticated.
//  def rawQueryFuture(params: Seq[(String, String)], logFilter: SimpleFilter[HttpRequest, HttpResponse]): Future[String] = {
//    // Ugly
//    val qse = queryString(params ++
//      logger.queryIdToken.map("magicLoggingToken" -> _).toList)
//
//    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpVerb.GET, qse.toString)
//    // Here be dragons! If you have multiple backends with shared IPs this could very well explode
//    // but finagle doesn't seem to properly set the http host header for http/1.1
//    request.addHeader(HttpHeaders.Names.HOST, servers.head)
//    val loggedClient = logFilter andThen client
//    (loggedClient(request)).map(response => {
//      response.getStatus match {
//        case HttpResponseStatus.OK => response.getContent.toString(CharsetUtil.UTF_8)
//        case status => throw SolrResponseException(status.getCode, status.getReasonPhrase, solrName, qse.toString)
//      }
//    })
//  }

}
