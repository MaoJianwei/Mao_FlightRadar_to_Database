package com.maojianwei.database;

import javax.xml.datatype.DatatypeConfigurationException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseDriver {

    private static final String SQL_COMMA = ",";
    private static final String SQL_TABLE_EXIST = "table ADSB_DATA already exists";
    private static final String SQL_MYSQL_TABLE_EXIST = "Table 'adsb_data' already exists";
    private static final int DB_TRANSECTION_UPPER = 3000;

    private Connection dbConnection;
    private AtomicBoolean ready;
    private AtomicInteger transectionCount;
    private AtomicInteger workRecordCount;


    public DatabaseDriver() {
        dbConnection = null;
        ready = new AtomicBoolean(false);
        transectionCount = new AtomicInteger(DB_TRANSECTION_UPPER);
        workRecordCount = new AtomicInteger(0);
    }

    //Deprecated, 2016.04.10, just for function utility
//    public static void main(String args[]) {
//
//        PoetryDatabase db = new PoetryDatabase();
//        db.initDatabase("MaoPoetry.db");
//
//        db.testUtility();
//
//        db.releaseDatabase();
//    }

    //2016.04.10, just for function utility
//    private boolean testUtility(){
//        if (!ready.get()) {
//            return false;
//        }
//
//        try {
//            Statement statement = dbConnection.createStatement();
//
//            ResultSet resultSet = statement.executeQuery("SELECT POEM FROM POETRY");
//            while(resultSet.next()){
//                String s = resultSet.getString("poem");
//                int a = 0;
//            }
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return true;
//    }


    public boolean initDatabase(String dbFileName) {

        if (ready.get()) {
            return true;
        }

        try {
//            Class.forName("org.sqlite.JDBC");
//            dbConnection = DriverManager.getConnection("jdbc:sqlite:" + dbFileName);

            Class.forName("com.mysql.jdbc.Driver").newInstance();
            dbConnection = DriverManager.getConnection("jdbc:mysql://day.maojianwei.com/Mao_ADSB_DATA", "root","2012210227");

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        if (initTable()) {
//            totalCount.set(readRowCount());
//            if(totalCount.get() >= 0){
            ready.set(true);
            return true;
//            }
        }
        return false;
    }

    public boolean releaseDatabase() {

        if (!ready.get()) {
            return true;
        }
        ready.set(false);


        try {
            dbConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        dbConnection = null;
//        totalCount.set(-1);
        return true;
    }

    private boolean initTable() {

        if (ready.get()) {
            return true;
        }

        try {
            Statement statement = dbConnection.createStatement();

//            System.out.println("CREATE TABLE ADSB_DATA(" +
//                    "ID INT primary key AUTOINCREMENT not null unique ," +
//                    "TransmissionType int," +                       // Field 2
//                    "HexIdent text," +                              // Field 5
//                    "GenDate DATE," +                               // Field 7
//                    "GenTime TIME," +                               // Field 8
//                    "Callsign text," +                              // Field 11
//                    "Altitude INT," +                              // Field 12
//                    "GroundSpeed INT," +                           // Field 13
//                    "Track INT," +                                 // Field 14
//                    "Latitude DOUBLE," +                              // Field 15
//                    "Longitude DOUBLE," +                             // Field 16
//                    "VerticalRate INT," +                          // Field 17
//                    "Squawk INT," +                                // Field 18
//                    "SquawkChange BOOLEAN," +                          // Field 19
//                    "Emergency BOOLEAN," +                             // Field 20
//                    "Ident BOOLEAN," +                                 // Field 21
//                    "IsOnGround BOOLEAN" +                             // Field 22
//                    ")");
            //todo - SQL
            statement.executeUpdate("CREATE TABLE ADSB_DATA(" +
                    "ID INTEGER primary key AUTO_INCREMENT not null unique ," +    // mysql - AUTO_INCREMENT
//                    "ID INTEGER primary key AUTOINCREMENT not null unique ," +    // sqlite - AUTOINCREMENT
                    "TransmissionType INTEGER," +                       // Field 2
                    "HexIdent text," +                              // Field 5
                    "GenDateUTC DATE," +                               // Field 7
                    "GenTimeUTC TIME," +                               // Field 8
                    "Callsign text," +                              // Field 11
                    "Altitude INTEGER," +                              // Field 12
                    "GroundSpeed DOUBLE," +                           // Field 13
                    "Track DOUBLE," +                                 // Field 14
                    "Latitude DOUBLE," +                              // Field 15
                    "Longitude DOUBLE," +                             // Field 16
                    "VerticalRate INTEGER," +                          // Field 17
                    "Squawk INTEGER," +                                // Field 18
                    "SquawkChange BOOLEAN," +                          // Field 19
                    "Emergency BOOLEAN," +                             // Field 20
                    "Ident BOOLEAN," +                                 // Field 21
                    "IsOnGround BOOLEAN" +                             // Field 22
                    ")");
            statement.close();

            return true;
        } catch (SQLException e) {
            if (e.getMessage().equals(SQL_TABLE_EXIST) || e.getMessage().equals(SQL_MYSQL_TABLE_EXIST)) {
                return true;
            } else {
                e.printStackTrace();
                return false;
            }
        } finally {
            try {
                dbConnection.setAutoCommit(false);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private int doTransection(String sqlQuery) {

        if (transectionCount.getAndDecrement() == 0) {
            try {
                dbConnection.commit();
                transectionCount.set(DB_TRANSECTION_UPPER - 1);
                workRecordCount.addAndGet(DB_TRANSECTION_UPPER);
                System.out.println(Calendar.getInstance().getTime() + "   " +workRecordCount);
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    dbConnection.rollback();
                    return -2;
                } catch (SQLException e1) {
                    e1.printStackTrace();
                    return -3;
                }
            }
        }

        try {
//            System.out.print(".");
            return dbConnection.prepareStatement(sqlQuery).executeUpdate();
        } catch (SQLException e) {
            System.out.println(sqlQuery);
            e.printStackTrace();
            return -1;
        }
    }

    public boolean insertEntry(String[] adsbData) {

        if (!ready.get()) {
            return false;
        }

        return 0 < doTransection("INSERT INTO ADSB_DATA VALUES(" +
                "NULL," +
                adsbData[1] + SQL_COMMA +
                adsbData[4] + SQL_COMMA +
                adsbData[6] + SQL_COMMA +
                adsbData[7] + SQL_COMMA +
                adsbData[10] + SQL_COMMA +
                adsbData[11] + SQL_COMMA +
                adsbData[12] + SQL_COMMA +
                adsbData[13] + SQL_COMMA +
                adsbData[14] + SQL_COMMA +
                adsbData[15] + SQL_COMMA +
                adsbData[16] + SQL_COMMA +
                adsbData[17] + SQL_COMMA +
                adsbData[18] + SQL_COMMA +
                adsbData[19] + SQL_COMMA +
                adsbData[20] + SQL_COMMA +
                adsbData[21] +
                ")");
    }
}
