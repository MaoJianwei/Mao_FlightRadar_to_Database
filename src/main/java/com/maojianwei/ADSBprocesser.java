package com.maojianwei;

public class ADSBprocesser {

    private static final String MSG_SPLITTER= ",";
    private static final String MSG_NULL= "";
    private static final String MSG_TRUE= "-1";
    private static final String MSG_FALSE= "0";
    private static final String DATA_NULL= "NULL";
    private static final String DATA_QUOTE= "\"";
    private static final String DATA_TRUE= "TRUE";
    private static final String DATA_FALSE= "FALSE";

    private FRTDcore core;

    public void setFRTDcore(FRTDcore core) {
        this.core = core;
    }

    public void processADSBmsg(String msg){

        String [] adsbData = msg.split(MSG_SPLITTER);

        int msgTypeCode = Integer.valueOf(adsbData[1]);

        //ICAO_HexIdent
        adsbData[4] = DATA_QUOTE + adsbData[4] + DATA_QUOTE;
        //TODO - DATE & TIME
        //Callsign
        adsbData[10] = adsbData[10].equals(MSG_NULL) ? DATA_NULL : DATA_QUOTE + adsbData[10] + DATA_QUOTE;
        //Altitude
        adsbData[11] = adsbData[11].equals(MSG_NULL) ? DATA_NULL : adsbData[11];
        //GroundSpeed
        adsbData[12] = adsbData[12].equals(MSG_NULL) ? DATA_NULL : adsbData[12];
        //Track
        adsbData[13] = adsbData[13].equals(MSG_NULL) ? DATA_NULL : adsbData[13];
        //Latitude
        adsbData[14] = adsbData[14].equals(MSG_NULL) ? DATA_NULL : adsbData[14];
        //Longitude
        adsbData[15] = adsbData[15].equals(MSG_NULL) ? DATA_NULL : adsbData[15];
        //VerticalRate
        adsbData[16] = adsbData[16].equals(MSG_NULL) ? DATA_NULL : adsbData[16];
        //Squawk
        adsbData[17] = adsbData[17].equals(MSG_NULL) ? DATA_NULL : adsbData[17];

        //Alert (Squawk change)
        adsbData[18] = dealAlert(msgTypeCode, adsbData[18]);

        //Emergency
        adsbData[19] = dealEmergency(msgTypeCode, adsbData[19]);

        //SPI (Ident)
        adsbData[20] = dealIdent(msgTypeCode, adsbData[20]);

        //IsOnGround
        adsbData[21] = dealIsOnGround(msgTypeCode, adsbData[21]);

        System.out.println(adsbData);

        core.submitADSBdata(adsbData);
    }

    private String dealAlert(int msgTypeCode, String alertMsg) {
        if(msgTypeCode == 3 || msgTypeCode == 5 || msgTypeCode == 6) {
            //TODO - check and log - DATA_NULL is exceptional situation here!
            return alertMsg.equals(MSG_NULL) ? DATA_FALSE :
                    (alertMsg.equals(MSG_FALSE) ? DATA_FALSE :
                            (alertMsg.equals(MSG_TRUE) ? DATA_TRUE : DATA_NULL));
        } else {
            return DATA_NULL;
        }
    }

    private String dealEmergency(int msgTypeCode, String emerMsg) {
        if(msgTypeCode == 3 || msgTypeCode == 6) {
            //TODO - check and log - DATA_NULL is exceptional situation here!
            return emerMsg.equals(MSG_NULL) ? DATA_FALSE :
                    (emerMsg.equals(MSG_FALSE) ? DATA_FALSE :
                            (emerMsg.equals(MSG_TRUE) ? DATA_TRUE : DATA_NULL));
        } else {
            return DATA_NULL;
        }
    }

    private String dealIdent(int msgTypeCode, String spiMsg) {
        if(msgTypeCode == 3 || msgTypeCode == 5 || msgTypeCode == 6) {
            //TODO - check and log - DATA_NULL is exceptional situation here!
            return spiMsg.equals(MSG_NULL) ? DATA_FALSE :
                    (spiMsg.equals(MSG_FALSE) ? DATA_FALSE :
                            (spiMsg.equals(MSG_TRUE) ? DATA_TRUE : DATA_NULL));
        } else {
            return DATA_NULL;
        }
    }

    private String dealIsOnGround(int msgTypeCode, String gndMsg) {
        //TODO - check and log - the last DATA_NULL is exceptional situation here!
        return gndMsg.equals(MSG_NULL) ? DATA_NULL :
                (gndMsg.equals(MSG_FALSE) ? DATA_FALSE :
                        (gndMsg.equals(MSG_TRUE) ? DATA_TRUE : DATA_NULL));
    }
}
