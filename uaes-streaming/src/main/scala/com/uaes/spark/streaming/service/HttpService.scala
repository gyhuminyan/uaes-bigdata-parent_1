package com.uaes.spark.streaming.service

import java.net.URLEncoder
import java.util

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.message.BasicNameValuePair
/**
  * Created by mzhang on 2017/10/24.
  */
class HttpService {
  def sendHttpRequest(url:String ,args : String) : Unit = {

    val client = new DefaultHttpClient
    val post = new HttpPost(url)
    //    post.addHeader("appid","YahooDemo")
    //    post.addHeader("query","umbrella")
    //    post.addHeader("results","10")

    val params = client.getParams
        params.setParameter("foo", "bar")

        val nameValuePairs = new util.ArrayList[NameValuePair](1)
        nameValuePairs.add(new BasicNameValuePair("registrationid", "123456789"));
        nameValuePairs.add(new BasicNameValuePair("accountType", "GOOGLE"));
        post.setEntity(new UrlEncodedFormEntity(nameValuePairs));

    // send the post request
    val response = client.execute(post)

  }
}
