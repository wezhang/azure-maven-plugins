/*
 * Copyright (c) Microsoft Corporation
 *
 * All rights reserved.
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.maven.spark.processes;

import com.google.common.net.HostAndPort;
import com.microsoft.azure.maven.spark.errors.SparkJobFinishedException;
import com.microsoft.azure.maven.spark.errors.SparkJobUploadArtifactException;
import com.microsoft.azure.maven.spark.events.MessageInfoType;
import com.microsoft.azure.maven.spark.events.SparkBatchJobSubmissionEvent;
import com.microsoft.azure.maven.spark.events.SparkBatchJobSubmittedEvent;
import com.microsoft.azure.maven.spark.job.SparkBatchJob;
import com.microsoft.azure.maven.spark.log.Logger;
import com.microsoft.azure.maven.spark.ux.IdeSchedulers;
import org.apache.commons.io.output.NullOutputStream;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Optional;

import static com.microsoft.azure.maven.spark.events.MessageInfoType.Info;


public class SparkBatchJobRemoteProcess extends Process implements Logger {
    @NonNull
    private IdeSchedulers schedulers;
    @NonNull
    private String artifactPath;
    @NonNull
    private final String title;
    @NonNull
    private final PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject;
    @NonNull
    private SparkJobLogInputStream jobStdoutLogInputSteam;
    @NonNull
    private SparkJobLogInputStream jobStderrLogInputSteam;
    @Nullable
    private Subscription jobSubscription;
    @NonNull
    private final SparkBatchJob sparkJob;
    @NonNull
    private final PublishSubject<SparkBatchJobSubmissionEvent> eventSubject = PublishSubject.create();
    private boolean isDestroyed = false;

    private boolean isDisconnected;

    public SparkBatchJobRemoteProcess(@NonNull IdeSchedulers schedulers,
                                      @NonNull SparkBatchJob sparkJob,
                                      @NonNull String artifactPath,
                                      @NonNull String title,
                                      @NonNull PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject) {
        this.schedulers = schedulers;
        this.sparkJob = sparkJob;
        this.artifactPath = artifactPath;
        this.title = title;
        this.ctrlSubject = ctrlSubject;

        this.jobStdoutLogInputSteam = new SparkJobLogInputStream("stdout");
        this.jobStderrLogInputSteam = new SparkJobLogInputStream("stderr");
    }

    /**
     * To Kill the remote job.
     *
     * @return is the remote Spark Job killed
     */
    public boolean killProcessTree() {
        return false;
    }

    /**
     * Is the Spark job session connected
     *
     * @return is the Spark Job log getting session still connected
     */
    public boolean isDisconnected() {
        return isDisconnected;
    }

    @Nullable
    public HostAndPort getLocalTunnel(int i) {
        return null;
    }

    @Override
    public OutputStream getOutputStream() {
        return new NullOutputStream();
    }

    @Override
    public InputStream getInputStream() {
        return jobStdoutLogInputSteam;
    }

    @Override
    public InputStream getErrorStream() {
        return jobStderrLogInputSteam;
    }

    @Override
    public int waitFor() {
        return 0;
    }

    @Override
    public int exitValue() {
        return 0;
    }

    @Override
    public void destroy() {
        getSparkJob().killBatchJob().subscribe(
                job -> log().trace("Killed Spark batch job " + job.getBatchId()),
                err -> log().warn("Got error when killing Spark batch job", err),
                () -> {}
        );

        this.isDestroyed = true;

        this.disconnect();
    }

    @NonNull
    public SparkBatchJob getSparkJob() {
        return sparkJob;
    }

    public Optional<Subscription> getJobSubscription() {
        return Optional.ofNullable(jobSubscription);
    }

    public void start() {
        // Build, deploy and wait for the job done.
//        jobSubscription = prepareArtifact()
        jobSubscription = Observable.just(sparkJob)
                .flatMap(this::submitJob)
                .flatMap(this::awaitForJobStarted)
                .flatMap(this::attachInputStreams)
                .flatMap(this::awaitForJobDone)
                .subscribe(sdPair -> {
                    if (sparkJob.isSuccess(sdPair.getKey())) {
                        ctrlInfo("");
                        ctrlInfo("========== RESULT ==========");
                        ctrlInfo("Job run successfully.");
                    } else {
                        ctrlInfo("");
                        ctrlInfo("========== RESULT ==========");
                        ctrlError("Job state is " + sdPair.getKey());
                        ctrlError("Diagnostics: " + sdPair.getValue());
                    }
                }, err -> {
                    if (err instanceof SparkJobFinishedException || err.getCause() instanceof SparkJobFinishedException) {
                        // If we call destroy() when job is dead, we will get exception with `job is finished` error message
                        ctrlError("Job is already finished.");
                        isDestroyed = true;
                        disconnect();
                    } else {
                        ctrlError(err.getMessage());
                        destroy();
                    }
                }, () -> {
                    disconnect();
                });
    }

    @NonNull
    private Observable<? extends SparkBatchJob> awaitForJobStarted(@NonNull SparkBatchJob job) {
        return job.awaitStarted()
                .map(state -> job);
    }

    private Observable<? extends SparkBatchJob> attachJobInputStream(SparkJobLogInputStream inputStream, SparkBatchJob job) {
        return Observable.just(inputStream)
                .map(stream -> stream.attachJob(job))
                .subscribeOn(schedulers.processBarVisibleAsync("Attach Spark batch job outputs " + inputStream.getLogType()));
    }

    public void disconnect() {
        this.isDisconnected = true;

        this.ctrlSubject.onCompleted();
        this.eventSubject.onCompleted();

        this.getJobSubscription().ifPresent(Subscription::unsubscribe);
    }

    protected void ctrlInfo(String message) {
        ctrlSubject.onNext(new SimpleImmutableEntry<>(Info, message));
    }

    protected void ctrlError(String message) {
        ctrlSubject.onNext(new SimpleImmutableEntry<>(MessageInfoType.Error, message));
    }

    @NonNull
    public PublishSubject<SparkBatchJobSubmissionEvent> getEventSubject() {
        return eventSubject;
    }

    protected Observable<SparkBatchJob> startJobSubmissionLogReceiver(SparkBatchJob job) {
        return job.getSubmissionLog()
                .doOnNext(ctrlSubject::onNext)
                // "ctrlSubject::onNext" lead to uncaught exception
                // while "ctrlError" only print error message in console view
                .doOnError(err -> ctrlError(err.getMessage()))
                .lastOrDefault(null)
                .map((@Nullable SimpleImmutableEntry<MessageInfoType, String> messageTypeText) -> job);
    }

    // Build and deploy artifact
    protected Observable<? extends SparkBatchJob> prepareArtifact() {
        return getSparkJob()
                .deploy(artifactPath)
                .onErrorResumeNext(err -> {
                    Throwable rootCause = err.getCause() != null ? err.getCause() : err;
                    return Observable.error(new SparkJobUploadArtifactException("Failed to upload Spark application artifacts: " + rootCause.getMessage(), rootCause));
                })
                .subscribeOn(schedulers.processBarVisibleAsync("Deploy the jar file into cluster"));
    }

    protected Observable<? extends SparkBatchJob> submitJob(SparkBatchJob sparkJob) {
        return sparkJob
                .submit()
                .subscribeOn(schedulers.processBarVisibleAsync("Submit the Spark batch job"))
                .flatMap(this::startJobSubmissionLogReceiver)   // To receive the Livy submission log
                .doOnNext(job -> eventSubject.onNext(new SparkBatchJobSubmittedEvent(job)));
    }

    @NonNull
    public IdeSchedulers getSchedulers() {
        return schedulers;
    }

    @NonNull
    public String getTitle() {
        return title;
    }

    private Observable<? extends SparkBatchJob> attachInputStreams(SparkBatchJob job) {
        return Observable.zip(
                attachJobInputStream((SparkJobLogInputStream) getErrorStream(), job),
                attachJobInputStream((SparkJobLogInputStream) getInputStream(), job),
                (job1, job2) -> job);
    }

    Observable<SimpleImmutableEntry<String, String>> awaitForJobDone(SparkBatchJob runningJob) {
        return runningJob.awaitDone()
                .subscribeOn(schedulers.processBarVisibleAsync("Spark batch job " + getTitle() + " is running"))
                .flatMap(jobStateDiagnosticsPair -> runningJob
                        .awaitPostDone()
                        .subscribeOn(schedulers.processBarVisibleAsync(
                                "Waiting for " + getTitle() + " log aggregation is done"))
                        .map(any -> jobStateDiagnosticsPair));
    }

    @NonNull
    public PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> getCtrlSubject() {
        return ctrlSubject;
    }

    public boolean isDestroyed() {
        return isDestroyed;
    }
}
