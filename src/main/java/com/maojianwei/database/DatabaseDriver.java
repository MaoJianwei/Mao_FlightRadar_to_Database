package com.maojianwei.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseDriver {
    private Connection dbConnection;
    private AtomicBoolean ready;
    private AtomicInteger totalCount;


    public DatabaseDriver() {
        dbConnection = null;
        ready = new AtomicBoolean(false);
        totalCount = new AtomicInteger(-1);
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
            Class.forName("org.sqlite.JDBC");
            dbConnection = DriverManager.getConnection("jdbc:sqlite:" + dbFileName);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        if(initTable()){
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

            System.out.println("CREATE TABLE ADSB_DATA(" +
                    "ID INT primary key AUTOINCREMENT not null unique ," +
                    "TransmissionType int," +                       // Field 2
                    "HexIdent text," +                              // Field 5
                    "GenDate DATE," +                               // Field 7
                    "GenTime TIME," +                               // Field 8
                    "Callsign text," +                              // Field 11
                    "Altitude INT," +                              // Field 12
                    "GroundSpeed INT," +                           // Field 13
                    "Track INT," +                                 // Field 14
                    "Latitude DOUBLE," +                              // Field 15
                    "Longitude DOUBLE," +                             // Field 16
                    "VerticalRate INT," +                          // Field 17
                    "Squawk INT," +                                // Field 18
                    "SquawkChange BOOLEAN," +                          // Field 19
                    "Emergency BOOLEAN," +                             // Field 20
                    "Ident BOOLEAN," +                                 // Field 21
                    "IsOnGround BOOLEAN" +                             // Field 22
                    ")");
            //todo - SQL
            statement.executeUpdate("CREATE TABLE ADSB_DATA(" +
                    "ID INTEGER primary key AUTOINCREMENT not null unique ," +
                    "TransmissionType INTEGER," +                       // Field 2
                    "HexIdent text," +                              // Field 5
                    "GenDate DATE," +                               // Field 7
                    "GenTime TIME," +                               // Field 8
                    "Callsign text," +                              // Field 11
                    "Altitude INTEGER," +                              // Field 12
                    "GroundSpeed INTEGER," +                           // Field 13
                    "Track INTEGER," +                                 // Field 14
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

            if (e.getMessage().equals("table POETRY already exists")) {
                return true;
            } else {
                e.printStackTrace();
                return false;
            }
        }
    }


//    public boolean insertEntry(String [] adsbData) {
//
//        if (!ready.get()) {
//            return false;
//        }
//
////        if(totalCount.get() < 0){
////            return false;
////        }
//
//        try {
//            Statement statement = dbConnection.createStatement();
//            int update = statement.executeUpdate(
//                    "INSERT INTO POETRY VALUES(" +
////                            totalCount.incrementAndGet() + "," +
//                            "'" + poetry.getTitle() + "'," +
//                            "'" + poetry.getDynasty() + "'," +
//                            "'" + poetry.getPoet() + "'," +
//                            "'" + poetry.getPoem() + "'" +
//                            ")");
//            statement.close();
//            return update > 0;
//        } catch (SQLException e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
}
