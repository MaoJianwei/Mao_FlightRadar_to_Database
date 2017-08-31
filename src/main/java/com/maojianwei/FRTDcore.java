package com.maojianwei;

import com.maojianwei.database.DatabaseDriver;
import com.maojianwei.network.FlightRadarNetworkManager;

import java.net.UnknownHostException;

/**
 * flight-radar-to-data-warehouse-core, called FRTDcore for short.
 */
public class FRTDcore
{
    private DatabaseDriver database;
    private ADSBprocesser adsbProcesser;
    private FlightRadarNetworkManager networkManager;

    public static void main( String[] args ) throws InterruptedException, UnknownHostException {
        System.out.println( "Hello World!" );

        FRTDcore core = new FRTDcore();
        core.debugNetwork();

        while(true) {
                Thread.sleep(1000);
        }
    }


    private void debugNetwork() throws UnknownHostException {
        database = new DatabaseDriver();
        database.initDatabase("d:/adsb.db");

        adsbProcesser = new ADSBprocesser();
        adsbProcesser.setFRTDcore(this);

        networkManager = new FlightRadarNetworkManager();
        networkManager.setADSBprocesser(adsbProcesser);

        networkManager.connectToFlightRadar("flight.maojianwei.com", 33333);
    }

    // interface for ADSBprocesser
    public void submitADSBdata(String [] adsbData) {

    }
}
