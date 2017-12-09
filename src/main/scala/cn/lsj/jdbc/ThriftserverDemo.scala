//package cn.lsj.jdbc
//
///**
// *
// */
//object ThriftserverDemo {
//  def main(args: Array[String]):Unit= {
//    //add driver
//    val driver="org.apache.hive.jdbc.HiveDriver"
//    Class.forName(driver)
//
//    //get connection
//    val (url,username,userpasswd)=("jdbc:hive2://linux-hadoop3.ibeifeng.com:10000","beifeng","beifeng")
//    val connection=DriverManager.getConnection(url,username,userpasswd)
//
//    //get statement
//    connection.prepareStatement("use db_emp").execute()
//    val sql="select * from dept d join emp e on d.deptno=e.deptno"
//    val statement=connection.prepareStatement(sql)
//
//    //get result
//    val rs=statement.executeQuery()
//    while(rs.next()){
//      println(s"${rs.getString(1)}:${rs.getString(2)}")
//    }
//
//    //close
//    rs.close()
//    statement.close()
//    connection.close()
//  }
//}