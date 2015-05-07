package com.timgroup.statsd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.concurrent.*;


public class NonBlockingUdpSender {

    private final Charset encoding;
    private final DatagramChannel clientChannel;
    private final InetSocketAddress address;
    private final ExecutorService executor;
    private StatsDClientErrorHandler handler;
    private final BlockingQueue<String> queue;
    private int packetSizeBytes;

    public NonBlockingUdpSender(String hostname, int port, int packetSizeBytes, Charset encoding, StatsDClientErrorHandler handler) throws IOException {
        this.queue = new LinkedBlockingQueue<String>();
        this.encoding = encoding;
        this.handler = handler;
        this.clientChannel = DatagramChannel.open();
        this.address = new InetSocketAddress(hostname, port);
        this.clientChannel.connect(address);
        this.packetSizeBytes = packetSizeBytes;

        this.executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            final ThreadFactory delegate = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable runnable) {
                Thread result = delegate.newThread(runnable);
                result.setName("StatsD-" + result.getName());
                result.setDaemon(true);
                return result;
            }
        });

        this.executor.submit(new QueueConsumer());
    }

    public void stop() {
        try {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            handler.handle(e);
        }
        finally {
            if (clientChannel != null) {
                try {
                    clientChannel.close();
                }
                catch (Exception e) {
                    handler.handle(e);
                }
            }
        }
    }

    public void send(final String message) {
        queue.offer(message);
    }


    private class QueueConsumer implements Runnable {
        private final ByteBuffer sendBuffer = ByteBuffer.allocate(packetSizeBytes);

        @Override public void run() {
            while(!executor.isShutdown()) {
                try {
                    String message = queue.poll(1, TimeUnit.SECONDS);
                    if(message != null) {
                        byte[] data = message.getBytes();
                        if(sendBuffer.remaining() < (data.length + 1)) {
                            blockingSend();
                        }
                        if(sendBuffer.position() > 0) {
                            sendBuffer.put( (byte) '\n');
                        }
                        sendBuffer.put(data);
                        if(queue.peek() == null) {
                            blockingSend();
                        }
                    }
                } catch (Exception e) {
                    handler.handle(e);
                }
            }
        }

        private void blockingSend() throws IOException {
            int sizeOfBuffer = sendBuffer.position();
            sendBuffer.flip();
            int sentBytes = clientChannel.send(sendBuffer, address);
            sendBuffer.limit(sendBuffer.capacity());
            sendBuffer.rewind();

            if (sizeOfBuffer != sentBytes) {
                handler.handle(
                        new IOException(
                                String.format(
                                        "Could not send entirely stat %s to host %s:%d. Only sent %d bytes out of %d bytes",
                                        sendBuffer.toString(),
                                        address.getHostName(),
                                        address.getPort(),
                                        sentBytes,
                                        sizeOfBuffer)));
            }
        }
    }
}