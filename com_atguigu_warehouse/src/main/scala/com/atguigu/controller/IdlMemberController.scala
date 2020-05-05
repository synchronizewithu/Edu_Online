package com.atguigu.controller

import com.atguigu.service.IdlMemberService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object IdlMemberController {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("idl_member_import")
    //    .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    IdlMemberService.importMember(sparkSession, "20190706") //根据用户信息聚合用户表数据
    //    IdlMemberService.importMemberUseApi(sparkSession)
  }
}
