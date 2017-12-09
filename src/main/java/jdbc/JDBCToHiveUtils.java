package jdbc;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by lsj on 2017/12/6.
 */
public class JDBCToHiveUtils {
    private static String driverName ="org.apache.hive.jdbc.HiveDriver";
    //填写hive的IP，之前在配置文件中配置的IP
    private static String Url="jdbc:hive2://192.168.133.128:10000/";
    private static Connection conn;
    public static Connection getConnnection()
    {
        try
        {
            Class.forName(driverName);
            //此处的用户名一定是有权限操作HDFS的用户，否则程序会提示"permission deny"异常
            conn = DriverManager.getConnection(Url,"hive","");
        }
        catch(ClassNotFoundException e)  {
            e.printStackTrace();
            System.exit(1);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    public static PreparedStatement prepare(Connection conn, String sql) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ps;
    }
}