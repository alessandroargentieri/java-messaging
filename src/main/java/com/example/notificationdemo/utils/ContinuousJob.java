package com.example.notificationdemo.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ContinuosJob abstract class implements a periodic job to be executed without blocking any Thread.
 */
public abstract class ContinuousJob {

    protected ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    protected long interval = 1000;

    /**
     * Executes the Job in a cyclic way pausing any defined interval without blocking any Thread.
     */
    public void start() {
        scheduler.scheduleAtFixedRate(() -> doWork(),0, this.interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the periodic execution of the Job.
     */
    public void stop() {
        scheduler.shutdown();
    }

    /**
     * Sets the interval to wait until the next iteration of the Job.
     * @param interval
     */
    public void setInterval(long interval) {
        this.interval = interval;
    }

    /**
     * Defines the job to be done periodically at the specified interval.
     */
    public abstract void doWork();

}
