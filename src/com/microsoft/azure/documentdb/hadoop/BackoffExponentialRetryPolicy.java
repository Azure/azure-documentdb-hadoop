package com.microsoft.azure.documentdb.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.microsoft.azure.documentdb.DocumentClientException;

public class BackoffExponentialRetryPolicy {
    private static final int REQUEST_RATE_TOO_LARGE = 429;
    
    private static final Log LOG = LogFactory.getLog(BackoffExponentialRetryPolicy.class);
    
    private final long defaultRetryInSeconds = 3;
    
    private final int retryAdditiveMultiplier = 500;
    
    private int currentAttemptCount = 0;

    private long retryAfterInMilliseconds = 0;
    
    public BackoffExponentialRetryPolicy() {
        
    }
    
    public int getCurrentAttempt() {
        return this.currentAttemptCount;
    }
    
    public boolean shouldRetry(){
        return true;
    }

    /**
     * Report that an error has occured and sleeps if the error is retriable
     * @param exception
     */
    public void errorOccured(Exception exception) {
        if (!isExceptionRetriable(exception)) {
            throw new IllegalStateException("Exception not retriable: " + exception.getMessage(), exception);
        }
        
        waitUntilNextTry();
    }
    
    private void waitUntilNextTry() {
        try {
            LOG.info("Trial number: " + this.currentAttemptCount + ", retrying after: " + this.getRetryAfterInMilliseconds());
            Thread.sleep(this.getRetryAfterInMilliseconds());
        } catch (InterruptedException ignored) {
        }
    }
    
    private long getRetryAfterInMilliseconds() {
        return this.retryAfterInMilliseconds;
    }

    private boolean isExceptionRetriable(Exception exception) {
        this.retryAfterInMilliseconds = 0;

        if (this.CheckIfRetryNeeded(exception)) {
            this.currentAttemptCount++;
            return true;
        } else {
            return false;
        }
    }

    private boolean CheckIfRetryNeeded(Exception exception) {
        this.retryAfterInMilliseconds = 0;

        if(exception instanceof IllegalStateException) {
            exception = (Exception) exception.getCause();
        }
        
        if (exception instanceof DocumentClientException) {
            DocumentClientException dce = (DocumentClientException) exception;

            if (dce.getStatusCode() == REQUEST_RATE_TOO_LARGE) {
                this.retryAfterInMilliseconds = dce.getRetryAfterInMilliseconds() + this.currentAttemptCount * this.retryAdditiveMultiplier;

                if (this.retryAfterInMilliseconds == 0) {
                    // we should never reach here as BE should turn non-zero of
                    // retry delay.
                    this.retryAfterInMilliseconds = this.defaultRetryInSeconds * 1000;
                }

                return true;
            }
        }

        return false;
    }
}
