package com.maojianwei.network;

import com.maojianwei.ADSBprocesser;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class FlightRadarReceiver extends ChannelInboundHandlerAdapter {

    private ADSBprocesser adsbProcesser;

    private static final String ADSB_SPLITER = ",";
    private static final String ADSB_IDENTIFIER = "MSG";

    public FlightRadarReceiver(ADSBprocesser adsbProcesser) {
        this.adsbProcesser = adsbProcesser;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
//        channel = ctx.channel();
//        log.info("go into channelActive, state: {}, channel's string: {}",
//                state,
//                channel.toString());
//        setState(WAIT_HELLO);
//
//        if(isRoleClient) {
//            state.sendHelloMessage(ctx);
//        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
//        log.info("go into channelInactive, state: {}", state);
//
//        setState(ENDING);
//        //TODO - Release resource
//
//        log.info("{} will announce disconnected, state: {}", maoProtocolNode.getAddressStr(), state);
//        maoProtocolNode.announceDisConnected();
//
//        // TODO: 2016/10/20 try to reconnect just while error occur.
//        if(isRoleClient){
//            log.info("{} will report clientNodeDown, state: {}", maoProtocolNode.getAddressStr(), state);
//            controller.clientReportNodeDown(this.maoProtocolNode.getAddressInet());
//        }
//
//        setState(END);
//        log.info("{} release over, good day! state: {}", maoProtocolNode.getAddressStr(), state);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        String adsb = (String) msg;
        if(verifyADSB(adsb)) {
            adsbProcesser.processADSBmsg(adsb);
        }
    }

    private boolean verifyADSB(String adsbMsg) {

        //todo - check performance issue

        String [] elements = adsbMsg.split(ADSB_SPLITER);
        return (elements.length == 22) && elements[0].equals(ADSB_IDENTIFIER);
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
