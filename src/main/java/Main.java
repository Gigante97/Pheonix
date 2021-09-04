import java.sql.*;

public class Main {
    static final String JDBC_DRIVER = "hbase01.preprod.milk.crpt.tech";
    static final String IP = "<placeholder>";
    static final String PORT = "2181";
    static final String DB_URL = "jdbc:phoenix:hbase01.preprod.milk.crpt.tech";

    public static void main(String[] args) {
        Connection conn = null;
        Statement st = null;

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

            System.out.println("Connecting to database..");

            conn = DriverManager.getConnection(DB_URL);

            System.out.println("Creating statement...");

            st = conn.createStatement();
            String sql;
            sql = "SELECT \"c\" FROM PREPROD_WATER.TBL_JTI_TRACE_CIS WHERE \"c\" ='?'";
            ResultSet rs = st.executeQuery(sql);

            while(rs.next()) {
                String did = rs.getString(1);
                System.out.println("Did found: " + did);
            }

            rs.close();
            st.close();
            conn.close();

        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            // finally block used to close resources
            try {
                if (st != null)
                    st.close();
            } catch (SQLException se2) {
            } // nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            } // end finally try
        } // end try
        System.out.println("Goodbye!");
    }
}