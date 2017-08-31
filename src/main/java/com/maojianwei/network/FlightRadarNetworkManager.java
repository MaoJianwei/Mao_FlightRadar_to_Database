package com.maojianwei.network;

import com.maojianwei.ADSBprocesser;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;

public class FlightRadarNetworkManager {

    private static final int CLIENT_TIMEOUT_DELAY = 1000;

    EventLoopGroup eventGroup;

    ADSBprocesser adsbProcesser;

    public FlightRadarNetworkManager() {
        eventGroup = new NioEventLoopGroup();
    }

//    public void initNetwork(ADSBprocesser adsbProcesser) {
//        setADSBprocesser(adsbProcesser);
//    }

    public void setADSBprocesser (ADSBprocesser adsbProcesser) {
        this.adsbProcesser = adsbProcesser;
    }

    public void connectToFlightRadar(String flightRadarAddr, int flightRadarPort) {

        Bootstrap boot = new Bootstrap()
                .group(eventGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) // TODO - CHECK
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CLIENT_TIMEOUT_DELAY)
                .option(ChannelOption.SO_KEEPALIVE, false) // TODO - VERITY - ATTENTION !!!
                .handler(new FlightRadarChannelInitializer());

        boot.connect(flightRadarAddr, flightRadarPort).addListener(new CheckConnectResult(flightRadarAddr, flightRadarPort));
    }

    private class FlightRadarChannelInitializer extends ChannelInitializer<SocketChannel> {

        public FlightRadarChannelInitializer( ) {}

        @Override
        public void initChannel(SocketChannel ch) {
            try {
                ChannelPipeline p = ch.pipeline();
                //Attention - assume that if we use LengthFieldBasedFrameDecoder, the frame is certainly unbroken.
                p.addLast(
                        new LineBasedFrameDecoder(Integer.MAX_VALUE),
                        new StringDecoder(),
                        new FlightRadarReceiver(adsbProcesser));

            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }


    private class CheckConnectResult implements ChannelFutureListener {

        String flightRadarAddr;
        int flightRadarPort;

        public CheckConnectResult(String flightRadarAddr, int flightRadarPort){
            this.flightRadarAddr = flightRadarAddr;
            this.flightRadarPort = flightRadarPort;
        }

        @Override
        public void operationComplete(ChannelFuture future) {
            if (!future.isSuccess()) {
//                log.warn("Exception while connecting {}, will re-connect in {} seconds",
//                        future.cause().getMessage(), CLIENT_SCHEDULE_DELAY);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                connectToFlightRadar(flightRadarAddr, flightRadarPort);
            }
        }
    }
}
