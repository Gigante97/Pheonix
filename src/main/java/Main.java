import java.io.FileNotFoundException;
import java.sql.*;

public class Main {
    static final int size =1000;
    static final int h = size/8;

    private static Connection connection;
    private static PreparedStatement pstmt;
    private static Statement stmt;

    static final String JDBC_DRIVER = "hbase01.preprod.milk.crpt.tech";
    static final String IP = "<placeholder>";
    static final String PORT = "2181";
    static final String DB_URL = "jdbc:phoenix:hbase01.preprod.milk.crpt.tech";

    public static void connect() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            System.out.println("Connecting to database..");
            connection = DriverManager.getConnection(DB_URL);
            System.out.println("Creating statement...");
            stmt = connection.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void disconnect(){
        try {
            connection.close();
        }catch (SQLException throwables){
            throwables.printStackTrace();
        }
    }

    public static void main(String[] args) throws FileNotFoundException, SQLException {
        connect();
        GenerateArray generateArray =new GenerateArray();
        multiThreads(generateArray.generateArrays());

    }

    private synchronized static void multiThreads(String[] array) throws SQLException {
        int st =1;

        String[] firstArray=new String[h];
        String[] secondArray=new String[h];
        String[] threeArray=new String[h];
        String[] fourArray=new String[h];
        String[] fiveArray=new String[h];
        String[] sixArray=new String[h];
        String[] sevenArray=new String[h];
        String[] eightArray=new String[h];


        System.arraycopy(array, 0, firstArray, 0, h);
        System.arraycopy(array, h, secondArray, 0, h);
        System.arraycopy(array, (2*h), threeArray, 0, h);
        System.arraycopy(array, (3*h), fourArray, 0, h);
        System.arraycopy(array, (4*h), fiveArray, 0, h);
        System.arraycopy(array, (5*h), sixArray, 0, h);
        System.arraycopy(array, (6*h), sevenArray, 0, h);
        System.arraycopy(array, (7*h), eightArray, 0, h);

        connection.setAutoCommit(false);

        pstmt = connection.prepareStatement("UPSERT INTO PREPROD_WATER.TBL_JTI_TRACE_CIS (\"c\",\"st\") VALUES(?,?)");
        long t =System.currentTimeMillis();
        Thread d = new Thread(()-> System.out.println(1));
        d.setDaemon(true);
        d.start();
        Thread doFirst = new Thread(new Runnable() {
            @Override
            public synchronized void run() {
                synchronized (array){
                    for (int i=0;i<firstArray.length;i++) {
                        try {
                            pstmt.setString(1, firstArray[i]);
                            pstmt.setInt(2, st);
                            pstmt.addBatch();
                            pstmt.executeBatch();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

                }}

        });

        Thread doSecond = new Thread(new Runnable() {
            @Override
            public synchronized void run() {
                synchronized (array){
                    for (int q=0;q<secondArray.length;q++) {
                        try {
                            pstmt.setString(1, secondArray[q]);
                            pstmt.setInt(2, st);
                            pstmt.addBatch();
                            pstmt.executeBatch();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

                }}

        });
        Thread doThree = new Thread(new Runnable() {
            @Override
            public synchronized void run() {
                synchronized (array) {
                    for (int w = 0; w < threeArray.length; w++) {
                        try {
                            pstmt.setString(1, threeArray[w]);
                            pstmt.setInt(2, st);
                            pstmt.addBatch();
                            pstmt.executeBatch();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

                }
            }
        });
        Thread doFour = new Thread(new Runnable() {
            @Override
            public synchronized void run() {
                synchronized (array) {
                    for (int a = 0; a < fourArray.length; a++) {
                        try {
                            pstmt.setString(1, fourArray[a]);
                            pstmt.setInt(2, st);
                            pstmt.addBatch();
                            pstmt.executeBatch();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

                }
            }

        });
        Thread doFive = new Thread(new Runnable() {
            @Override
            public synchronized void run() {
                synchronized (array) {
                    for (int f = 0; f < fiveArray.length; f++) {
                        try {
                            pstmt.setString(1, fiveArray[f]);
                            pstmt.setInt(2, st);
                            pstmt.addBatch();
                            pstmt.executeBatch();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

                }
            }

        });
        Thread doSix = new Thread(new Runnable() {
            @Override
            public synchronized void run() {
                synchronized (array) {
                    for (int b = 0; b < sixArray.length; b++) {
                        try {
                            pstmt.setString(1, sixArray[b]);
                            pstmt.setInt(2, st);
                            pstmt.addBatch();
                            pstmt.executeBatch();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }

        });
        Thread doSeven = new Thread(new Runnable() {
            @Override
            public synchronized void run() {
                synchronized (array) {
                    for (int l = 0; l < sevenArray.length; l++) {
                        try {
                            pstmt.setString(1, sevenArray[l]);
                            pstmt.setInt(2, st);
                            pstmt.addBatch();
                            pstmt.executeBatch();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

                }
            }

        });
        Thread doEight = new Thread(new Runnable() {
            @Override
            public synchronized void run() {
                synchronized (array) {
                    for (int p = 0; (p < (eightArray).length); p++) {
                        try {
                            pstmt.setString(1, eightArray[p]);
                            pstmt.setInt(2, st);
                            pstmt.addBatch();
                            pstmt.executeBatch();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

                }
            }

        });
        doFirst.start();
        doSecond.start();
        doThree.start();
        doFour.start();
        doFive.start();
        doSix.start();
        doSeven.start();
        doEight.start();

        try {
            doFirst.join();
            doSecond.join();
            doThree.join();
            doFour.join();
            doFive.join();
            doSix.join();
            doSeven.join();
            doEight.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            disconnect();
            System.out.println("Godbye!");
            System.out.println(System.currentTimeMillis()-t);
        }
    }
}