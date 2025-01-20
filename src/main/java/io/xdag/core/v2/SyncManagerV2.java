package io.xdag.core.v2;

import io.xdag.consensus.SyncManager;
import io.xdag.net.message.Message;

import java.time.Duration;

public interface SyncManagerV2 {

    /**
     * Starts sync manager, and sync blocks in [height, targetHeight).
     *
     * @param targetHeight
     *            the target height, exclusive
     */
    void start(long targetHeight);

    /**
     * Stops sync manager.
     */
    void stop();

    /**
     * Returns if this sync manager is running.
     *
     * @return
     */
    boolean isRunning();

    /**
     * Callback when a message is received from network.
     *
     * @param channel
     *            the channel where the message is coming from
     * @param msg
     *            the message
     */
    void onMessage(XdagChannel channel, Message msg);

    /**
     * Returns current synchronisation progress.
     *
     * @return a ${@link SyncManagerV2.Progress} object
     */
    SyncManagerV2.Progress getProgress();

    /**
     * This interface represents synchronisation progress
     */
    interface Progress {

        /**
         * @return the starting height of this sync process.
         */
        long getStartingHeight();

        /**
         * @return the current height of sync process.
         */
        long getCurrentHeight();

        /**
         * @return the target height of sync process.
         */
        long getTargetHeight();

        /**
         * @return the estimated time to complete this sync process. 30 days at maximum.
         */
        Duration getSyncEstimation();
    }
}
