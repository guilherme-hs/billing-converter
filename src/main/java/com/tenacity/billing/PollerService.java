package com.tenacity.billing;

/**
 * Represents an poller service
 */
public interface PollerService {

    //starts the Poller
    void run();

    //stops the Poller
    void stop();

    //verifies the Poller State
    boolean isStop();
}
