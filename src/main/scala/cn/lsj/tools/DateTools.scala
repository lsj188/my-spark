package cn.lsj.tools

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

object DateTools {
    /**
     * 1、获取今天日期
     **/
    def getNowDate(): String = {
        var now: Date = new Date()
        var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var hehe = dateFormat.format(now)
        hehe
    }

    /**
     * 2、获取昨天的日期
     **/
    def getYesterday(): String = {
        var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        var yesterday = dateFormat.format(cal.getTime())
        yesterday
    }

    /**
     * 日期计算
     **/
    def dayCalculate(s: String, d: Int): String = {
        var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var cal: Calendar = Calendar.getInstance()
        cal.setTime(DateTools.format2Date(s))
        cal.add(Calendar.DATE, d)
        var day = dateFormat.format(cal.getTime())
        day
    }

    /**
     * 3、获取本周开始日期
     **/
    def getNowWeekStart(): String = {
        var period: String = ""
        var cal: Calendar = Calendar.getInstance()
        var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
        //获取本周一的日期
        period = df.format(cal.getTime())
        period
    }

    /**
     * 4、获取本周末的时间
     **/
    def getNowWeekEnd(): String = {
        var period: String = ""
        var cal: Calendar = Calendar.getInstance();
        var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
        cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
        period = df.format(cal.getTime())
        period
    }


    /**
     * 5、本月的第一天
     **/
    def getNowMonthStart(): String = {
        var period: String = ""
        var cal: Calendar = Calendar.getInstance();
        var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.set(Calendar.DATE, 1)
        period = df.format(cal.getTime()) //本月第一天
        period
    }


    /**
     * 6、本月的最后一天
     **/
    def getNowMonthEnd(): String = {
        var period: String = ""
        var cal: Calendar = Calendar.getInstance();
        var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.set(Calendar.DATE, 1)
        cal.roll(Calendar.DATE, -1)
        period = df.format(cal.getTime()) //本月最后一天
        period
    }

    /**
     * 7、将时间戳转化成日期 * 时间戳是秒数，需要乘以1000l转化成毫秒
     **/
    def DateFormat(time: String): String = {
        var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var date: String = sdf.format(new Date((time.toLong * 1000l)))
        date
    }


    /**
     * 8、时间戳转化为时间，原理同上
     **/
    def timeFormat(time: String): String = {
        var sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
        var date: String = sdf.format(new Date((time.toLong * 1000l)))
        date
    }

    /**
     * 10计算时间差
     * 核心工作时间，迟到早退等的的处理
     **/
    def getCoreTime(start_time: String, end_Time: String) = {
        var df: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
        var begin: Date = df.parse(start_time)
        var end: Date = df.parse(end_Time)
        var between: Long = (end.getTime() - begin.getTime()) / 1000 //转化成秒
        var hour: Float = between.toFloat / 3600
        var decf: DecimalFormat = new DecimalFormat("#.00")
        decf.format(hour) //格式化

    }

    /**
     * 时间字串转日期
     **/
    def format2Date(s: String): Date = {
        var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        df.parse(s)
    }

    /**
     * 时间字串转日期
     **/
    def Date2formate(date: Date) = {
        var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        df.format(date)
    }

    /**
     * 测试
     **/
    def main(args: Array[String]) {
        println("现在时间：" + DateTools.getNowDate())
        println("昨天时间：" + DateTools.getYesterday())
        println("本周开始" + DateTools.getNowWeekStart())
        println("本周结束" + DateTools.getNowWeekEnd())
        println("本月开始" + DateTools.getNowMonthStart())
        println("本月结束" + DateTools.getNowMonthEnd())
        println("时间戳（1436457603）转时间：" + DateTools.timeFormat("1436457603"))
        println("时间戳（1436457603）转日期：" + DateTools.DateFormat("1436457603"))
        println("时间串（2015-01-01 23:23:23）转日期：" + DateTools.format2Date("2015-01-01 23:23:23"))
        println("时间(Date类型)转日期串：" + DateTools.Date2formate(DateTools.format2Date("2015-01-01 23:23:23")))
        println("时间计算：" + DateTools.dayCalculate("2015-01-01 23:23:23", 1))
    }

}