package cn.lsj

import cn.lsj.kafkaAndStream.RedisClient

/**
 * Created by lsj on 2017/9/20.
 */
object TestAll extends App {
//    (1 to 6).foreach(print)
//    for (i <- 1 to 6) println(i)
//    val a = List(1, 2, 3, 4, 5)
//    a.mkString(",")
//    val list = for (i <- 1 to 6) yield i
//    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    println(df.format(new Date()))
//    println(df.format(df.parse("2018-02-01 23:23:23")))

    //  97edfc08311c70143401745a03a50706
    val jedis = RedisClient.pool.getResource
    val uid = "97edfc08311c70143401745a03a50706"
    jedis.select(2)
    val map1=jedis.hgetAll(uid)
    if (map1.get("rowCount")==null)
    {println("取到了空的了")}

    println(map1)
    println("uid="+uid,"clickCount="+map1.get("clickCount"),"rowCount="+map1.get("rowCount"))
//    val clickCount = 1 + map1.get("clickCount").toInt
//    val rowCount = 1 + map1.get("rowCount").toInt
//    val map = Map("clickCount" -> clickCount.toString, "rowCount" -> rowCount.toString)
//
//    //选择数据库
//    jedis.select(2)
//    jedis.hmset(uid, mapAsJavaMap(map))
    RedisClient.pool.returnResource(jedis)
}
