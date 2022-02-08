package com.example.notificationdemo.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ContinuosJob abstract class implements a periodic job to be executed without blocking any {@link Thread}.
 */
public abstract class ContinuousJob {

    protected ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    protected long interval = 1000;

    /**
     * Executes the job in a cyclic way pausing any defined interval without blocking any {@link Thread}.
     */
    public void start() {
        scheduler.scheduleAtFixedRate(() -> doWork(),0, this.interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the job in a cyclic way pausing any defined interval without blocking any {@link Thread}.
     *
     * @param interval the interval of time amid the various cyclic executions
     */
    public void start(long interval) {
        scheduler.scheduleAtFixedRate(() -> doWork(),0, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the job in a cyclic way pausing any defined interval without blocking any {@link Thread}.
     *
     * @param job the job to be executed cyclically
     * @param interval the interval of time amid the various cyclic executions
     */
    public void start(Runnable job, long interval) {
        scheduler.scheduleAtFixedRate(job,0, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Executes the job in a cyclic way pausing any defined interval without blocking any {@link Thread}.
     *
     * @param job the job to be executed cyclically
     * @param interval the interval of time amid the various cyclic executions
     * @param initialDelay the initial delay for the first execution
     */
    public void start(Runnable job, long interval, long initialDelay) {
        scheduler.scheduleAtFixedRate(job,initialDelay, interval, TimeUnit.MILLISECONDS);
    }


    /**
     * Stops the periodic execution of the Job.
     */
    public void stop() {
        scheduler.shutdown();
    }

    /**
     * Sets the interval to wait until the next iteration of the Job.
     * @param interval the interval of time among the various cyclic executions
     */
    public void setInterval(long interval) {
        this.interval = interval;
    }

    /**
     * Defines the job to be done periodically at the specified interval.
     */
    public abstract void doWork();

}
