package com.example.notificationdemo.utils;

/**
 * ContinuousRunnable implements a repetitive Runnable instance.
 * The job that is defined gets repeated over and over any interval of time.
 * The default interval is set to 1 second in milliseconds but can be overwritten.
 */
public abstract class ContinuousRunnable implements Runnable {

    protected boolean stop;
    protected long interval = 1000;

    /**
     * Stops the continous execution.
     */
    public void stop() {
        this.stop = true;
    }

    /**
     * Sets the interval to wait until the next iteration of the Job
     * @param interval
     */
    protected void setInterval(long interval) {
        this.interval = interval;
    }

    /**
     * Defines the job to be done periodically at the specified interval.
     */
    protected abstract void doWork();

    @Override
    public void run() {
        this.stop = false;
        while(!stop) {
            doWork();
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
