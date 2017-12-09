package jdbc;
import java.sql.*;

/**
 * Created by
 * 通过jdbc访问Spark SQL
 * 该程序可以使其他应用程序访问使用Spark SQL,而Spark SQL又可以访问各种数据源(包括Hive及通过JDBC访问其他关系型数据库,这里起到一个桥梁的作用),本例是使用Java语言访问Spark SQL提供的server服务,然后Spark SQL访问的是hive中的数据.
 */
public class ThriftServerDemo {
    public static void main(String[] args) throws SQLException {

        //此处的用户名一定是有权限操作HDFS的用户，否则程序会提示"permission deny"异常
        Connection conn = JDBCToHiveUtils.getConnnection();
        String sql = "select * from info";
        System.out.println("Running:"+sql);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
//        PreparedStatement ps=JDBCToHiveUtils.prepare(conn, sql);
//        ResultSet rs=ps.executeQuery();
        int columns=rs.getMetaData().getColumnCount();
        while(rs.next()) {
            System.out.println("id: "+rs.getInt(1)+"\tname: "+rs.getString(2));
        }

    }
}