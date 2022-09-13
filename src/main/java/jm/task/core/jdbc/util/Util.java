package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    private static final String URL = "jdbc:mysql://localhost:3306/mydbtest";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static Connection connection;

    private static volatile Util instance;

    private Util (){
    }

    public Connection getConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            connection.setAutoCommit(false);

        } catch (ClassNotFoundException e) {
            System.err.println("Not download Driver mySQL");
        } catch (SQLException e) {
            System.err.println("Failed connection");
        }
        return connection;

    }
    public static Util getInstance(){
        Util localInstance = instance;
        if (localInstance == null){
            synchronized (Util.class){
                if (localInstance == null){
                    instance = localInstance = new Util();
                }
            }
        }
        return localInstance;
    }
}
