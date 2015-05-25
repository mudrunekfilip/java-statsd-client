package com.timgroup.statsd;


public class DummyErrorHandler implements StatsDClientErrorHandler {
    @Override
    public void handle(Exception exception) {
        Thread t = Thread.currentThread();
        t.getUncaughtExceptionHandler().uncaughtException(t, exception);
    }
}
