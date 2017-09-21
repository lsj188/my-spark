package cn.lsj

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by lsj on 2017/9/20.
 */
object TestAll extends App{
  (1 to 6).foreach(print)
  for (i <- 1 to 6) println(i)
  val a=List(1,2,3,4,5)
  a.mkString(",")

  val list=for (i<-1 to 6) yield i

  var df: SimpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
  println(df.format(new Date()))
  println(df.format(df.parse("2018-02-01 23:23:23")))

}
