
package com.timgroup.statsd;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


final class DummyStatsDServer {
    private final List<String> messagesReceived = new ArrayList<String>();
    private final DatagramSocket server;

    public DummyStatsDServer(int port, final Charset encoding, final int packetSize) {
        try {
            server = new DatagramSocket(port);
        } catch (SocketException e) {
            throw new IllegalArgumentException(e);
        }
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(!server.isClosed()) {
                    try {
                        final DatagramPacket packet = new DatagramPacket(new byte[packetSize], packetSize);
                        server.receive(packet);
                        for(String msg : new String(packet.getData(), encoding).split("\n")) {
                            messagesReceived.add(msg.trim());
                        }
                    } catch (IOException e) {
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    public void waitForMessage() {
        while (messagesReceived.isEmpty()) {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
            }
        }
    }

    public List<String> messagesReceived() {
        return new ArrayList<String>(messagesReceived);
    }

    public void stop() {
        server.close();
    }

}
