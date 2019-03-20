/*
 * Copyright (c) Microsoft Corporation
 * <p/>
 * All rights reserved.
 * <p/>
 * MIT License
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.maven.spark.job;

import com.microsoft.azure.maven.spark.events.MessageInfoType;
import org.checkerframework.checker.nullness.qual.NonNull;
import rx.Observable;
import rx.Observer;

import java.net.URI;
import java.util.AbstractMap.SimpleImmutableEntry;

public interface SparkBatchJob {
    /**
     * Getter of the base connection URI for HDInsight Spark Job service
     *
     * @return the base connection URI for HDInsight Spark Job service
     */
    URI getConnectUri();

    /**
     * Getter of the LIVY Spark batch job ID got from job submission
     *
     * @return the LIVY Spark batch job ID
     */
    int getBatchId();

    /**
     * Getter of the maximum retry count in RestAPI calling
     *
     * @return the maximum retry count in RestAPI calling
     */
    int getRetriesMax();

    /**
     * Setter of the maximum retry count in RestAPI calling
     * @param retriesMax the maximum retry count in RestAPI calling
     */
    void setRetriesMax(int retriesMax);

    /**
     * Getter of the delay seconds between tries in RestAPI calling
     *
     * @return the delay seconds between tries in RestAPI calling
     */
    int getDelaySeconds();

    /**
     * Setter of the delay seconds between tries in RestAPI calling
     * @param delaySeconds the delay seconds between tries in RestAPI calling
     */
    void setDelaySeconds(int delaySeconds);

    /**
     * Kill the batch job specified by ID
     *
     * @return the current instance observable for chain calling,
     *         Observable Error: IOException exceptions for networking connection issues related
     */
    Observable<? extends SparkBatchJob> killBatchJob();

    /**
     * Get Spark batch job driver host by ID
     *
     * @return Spark driver node host observable
     *         Observable Error: IOException exceptions for the driver host not found
     */
//    Observable<String> getSparkDriverHost();

    /**
     * Get Spark job driver log observable
     *
     * @param type the log type, such as `stderr`, `stdout`
     * @param logOffset the log offset that fetching would start from
     * @param size the fetching size, -1 for all.
     * @return the log and its starting offset pair observable
     */
//    @NonNull
//    Observable<SimpleImmutableEntry<String, Long>> getDriverLog(@NonNull String type, long logOffset, int size);

    /**
     * Get Spark job submission log observable
     *
     * @return the log type and content pair observable
     */
    @NonNull
    Observable<SimpleImmutableEntry<MessageInfoType, String>> getSubmissionLog();

    /**
     * Await the job started observable
     *
     * @return the job state string
     */
    @NonNull
    Observable<String> awaitStarted();

    /**
     * Await the job done observable
     *
     * @return the job state string and its diagnostics message
     */
    @NonNull
    Observable<SimpleImmutableEntry<String, String>> awaitDone();

    /**
     * Await the job post actions done, such as the log aggregation
     * @return the job post action status string
     */
    @NonNull
    Observable<String> awaitPostDone();

    /**
     * Get the job control messages observable
     *
     * @return the job control message type and content pair observable
     */
    @NonNull
    Observer<SimpleImmutableEntry<MessageInfoType, String>> getCtrlSubject();

    /**
     * Deploy the job artifact into cluster
     *
     * @param artifactPath the artifact to deploy
     * @return ISparkBatchJob observable
     *         Observable Error: IOException;
     */
    @NonNull
    Observable<? extends SparkBatchJob> deploy(@NonNull String artifactPath);

    /**
     * Create a batch Spark job and submit the job into cluster
     *
     * @return ISparkBatchJob observable
     *         Observable Error: IOException;
     */
    @NonNull
    Observable<? extends SparkBatchJob> submit();

    /**
     * Is the job done, success or failure
     *
     * @return true for success or failure
     */
    boolean isDone(@NonNull String state);

    /**
     * Is the job running
     *
     * @return true for running
     */
    boolean isRunning(@NonNull String state);

    /**
     * Is the job finished with success
     *
     * @return true for success
     */
    boolean isSuccess(@NonNull String state);
}
