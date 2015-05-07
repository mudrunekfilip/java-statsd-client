package com.timgroup.statsd;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class NonBlockingStatsDClientPerfTest {


    private static final int STATSD_SERVER_PORT = 17255;
    public static final int PACKET_SIZE_BYTES = 1500;
    private static final Random RAND = new Random();
    private final ExecutorService executor = Executors.newFixedThreadPool(20);

    private final NonBlockingStatsDClient client = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT, PACKET_SIZE_BYTES);
    private DummyStatsDServer server = new DummyStatsDServer(STATSD_SERVER_PORT, NonBlockingStatsDClient.STATS_D_ENCODING, PACKET_SIZE_BYTES);


    @After
    public void stop() throws Exception {
        client.stop();
        server.stop();
    }

    @Test(timeout=30000)
    public void perf_test() throws Exception {

        int testSize = 10000;
        for(int i = 0; i < testSize; ++i) {
            executor.submit(new Runnable() {
                public void run() {
                    client.count("mycount", RAND.nextInt());
                }
            });

            // to reduce network buffers overflow
            if (i % 20 == 0){
                Thread.sleep(1);
            }

        }

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);


        for(int i = 0; i < 20000 && server.messagesReceived().size() < testSize; i += 50) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {}
        }
        assertEquals(testSize, server.messagesReceived().size());
    }
}
