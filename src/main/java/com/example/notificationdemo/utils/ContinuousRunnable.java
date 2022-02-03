package com.example.notificationdemo.utils;

public abstract class ContinuousRunnable implements Runnable {

    protected boolean stop;
    protected long interval = 1000;

    public void stop() {
        this.stop = true;
    }

    protected void setInterval(long interval) {
        this.interval = interval;
    }

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
