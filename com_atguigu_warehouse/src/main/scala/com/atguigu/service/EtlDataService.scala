package com.atguigu.service

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSONObject
import com.atguigu.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object EtlDataService {
  /**
    * etl用户注册信息
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/odl/memberRegtype.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitoin => {
      partitoin.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val appkey = jsonObject.getString("appkey")
        val appregurl = jsonObject.getString("appregurl")
        val bdp_uuid = jsonObject.getString("bdp_uuid")
        val createtime = jsonObject.getString("createtime")
        val website = jsonObject.getString("dn")
        val domain = jsonObject.getString("webA")
        val isranreg = jsonObject.getString("isranreg")
        val regsource = jsonObject.getString("regsource")
        val regsourceName = regsource match {
          case "1" => "PC"
          case "2" => "Mobile"
          case "3" => "App"
          case "4" => "WeChat"
          case _ => "other"
        }
        val uid = jsonObject.getIntValue("uid")
        val time = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now())
        val websiteid = jsonObject.getIntValue("websiteid")
        (uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid, time, website)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("bdl.bdl_member_regtype")
  }

  /**
    * etl用户表数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/odl/Member.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val ad_id = jsonObject.getIntValue("ad_id")
        val birthday = jsonObject.getString("birthday")
        val website = jsonObject.getString("dn")
        val email = jsonObject.getString("email")
        val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
        val iconurl = jsonObject.getString("iconurl")
        val lastlogin = jsonObject.getString("lastlogin")
        val mailaddr = jsonObject.getString("mailaddr")
        val memberlevel = jsonObject.getString("memberlevel")
        val password = "******"
        val paymoney = jsonObject.getString("paymoney")
        val phone = jsonObject.getString("phone")
        val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
        val qq = jsonObject.getString("qq")
        val register = jsonObject.getString("register")
        val regupdatetime = jsonObject.getString("regupdatetime")
        val uid = jsonObject.getIntValue("uid")
        val unitname = jsonObject.getString("unitname")
        val userip = jsonObject.getString("userip")
        val zipcode = jsonObject.getString("zipcode")
        val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
        val time = dtf.format(LocalDate.now())
        (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
          register, regupdatetime, unitname, userip, zipcode, time, website)
      })
    }).toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("bdl.bdl_member")
  }

  /**
    * 导入广告表基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/odl/baseadlog.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val adid = jsonObject.getIntValue("adid")
        val adname = jsonObject.getString("adname")
        val website = jsonObject.getString("dn")
        (adid, adname, website)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_base_ad")
  }

  /**
    * 导入网站表基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/odl/baswewebsite.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val siteurl = jsonObject.getString("siteurl")
        val delete = jsonObject.getIntValue("delete")
        val createtime = jsonObject.getString("createtime")
        val creator = jsonObject.getString("creator")
        val website = jsonObject.getString("dn")
        (siteid, sitename, siteurl, delete, createtime, creator, website)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_base_website")
  }

  /**
    * 导入微信绑定数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemberWxboundLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/odl/memberwxbound.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
        val bindsource = jSONObject.getIntValue("bindsource")
        val boundtime = jSONObject.getString("boundtime")
        val website = jSONObject.getString("dn")
        val headimgurl = jSONObject.getString("headimgurl")
        val isdefault = jSONObject.getString("isdefault")
        val nickname = jSONObject.getString("nickname")
        val openid = jSONObject.getString("openid")
        val uid = jSONObject.getIntValue("uid")
        val unionid = jSONObject.getString("unionid")
        val time = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now())
        (uid, unionid, openid, nickname, isdefault, headimgurl, boundtime, bindsource, time, website)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("bdl.bdl_member_wxbound")
  }

  /**
    * 导入微信公众号基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlWxTypeLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/odl/WxType.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
        val app_id = jSONObject.getString("app_id")
        val bind_source = jSONObject.getIntValue("bind_source")
        val createtime = jSONObject.getString("createtime")
        val createtor = jSONObject.getString("atguigu")
        val website = jSONObject.getString("dn")
        val token_secret = jSONObject.getString("token_secret")
        val wx_name = jSONObject.getString("wx_name")
        val wx_no = jSONObject.getString("wx_no")
        (bind_source, app_id, token_secret, wx_name, wx_no, createtor, createtime, website)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_wxtype")
  }

  /**
    * 导入用户付款信息
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/odl/pcentermempaymoney.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
        val website = jSONObject.getString("dn")
        val paymoney = jSONObject.getString("paymoney")
        val uid = jSONObject.getIntValue("uid")
        val vip_id = jSONObject.getIntValue("vip_id")
        val site_id = jSONObject.getIntValue("siteid")
        val time = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now())
        (uid, paymoney, site_id, vip_id, time, website)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("bdl.bdl_pcentermempaymoney")
  }

  /**
    * 导入用户vip基础数据
    * @param ssc
    * @param sparkSession
    */
  def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/user/atguigu/odl/PcenterMemViplevel.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
        val discountval = jSONObject.getString("discountval")
        val website = jSONObject.getString("dn")
        val end_time = jSONObject.getString("end_time")
        val last_modify_time = jSONObject.getString("last_modify_time")
        val max_free = jSONObject.getString("max_free")
        val min_free = jSONObject.getString("min_free")
        val next_level = jSONObject.getString("next_level")
        val operator = jSONObject.getString("operator")
        val start_time = jSONObject.getString("start_time")
        val vip_id = jSONObject.getIntValue("vip_id")
        val vip_level = jSONObject.getString("vip_level")
        (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, website)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("bdl.bdl_vip_level")
  }
}
