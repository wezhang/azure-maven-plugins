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

import com.gargoylesoftware.htmlunit.*;
import com.gargoylesoftware.htmlunit.html.*;
import com.microsoft.azure.maven.spark.clusters.LivyCluster;
import com.microsoft.azure.maven.spark.clusters.YarnCluster;
import com.microsoft.azure.maven.spark.errors.HDIException;
import com.microsoft.azure.maven.spark.errors.SparkJobException;
import com.microsoft.azure.maven.spark.events.MessageInfoType;
import com.microsoft.azure.maven.spark.log.Logger;
import com.microsoft.azure.maven.spark.restapi.*;
import com.microsoft.azure.maven.spark.restapi.yarn.rm.App;
import com.microsoft.azure.maven.spark.restapi.yarn.rm.AppAttempt;
import com.microsoft.azure.maven.spark.restapi.yarn.rm.AppAttemptsResponse;
import com.microsoft.azure.maven.spark.restapi.yarn.rm.AppResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.CredentialsProvider;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownServiceException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.microsoft.azure.maven.spark.events.MessageInfoType.*;
import static java.lang.Thread.sleep;
import static rx.exceptions.Exceptions.propagate;

//@SuppressWarnings("argument.type.incompatible")
public class LivySparkBatch implements SparkBatchJob, Logger {
    public static final String WebHDFSPathPattern = "^(https?://)([^/]+)(/.*)?(/webhdfs/v1)(/.*)?$";
    public static final String AdlsPathPattern = "^adl://([^/.\\s]+\\.)+[^/.\\s]+(/[^/.\\s]+)*/?$";

    @Nullable
    private String currentLogUrl;
    @NonNull
    private Observer<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject;

    @Nullable
    private String getCurrentLogUrl() {
        return currentLogUrl;
    }

    private void setCurrentLogUrl(@Nullable String currentLogUrl) {
        this.currentLogUrl = currentLogUrl;
    }

//    public enum DriverLogConversionMode {
//        WITHOUT_PORT,
//        WITH_PORT,
//        ORIGINAL;
//
//        @Nullable
//        public static DriverLogConversionMode next(@Nullable DriverLogConversionMode current) {
//            List<DriverLogConversionMode> modes = Arrays.asList(DriverLogConversionMode.values());
//
//            if (current == null) {
//                return modes.get(0);
//            }
//
//            int found = modes.indexOf(current);
//
//            if (found + 1 >= modes.size()) {
//                return null;
//            }
//
//            return modes.get(found + 1);
//        }
//    }

    /**
     * The base connection URI for HDInsight Spark Job service, such as: http://livy:8998/batches
     */
    @Nullable
    private URI connectUri;

    /**
     * The base connection URI for HDInsight Yarn application service, such as: http://hn0-spark2:8088/cluster/app
     */
    @Nullable
    private URI yarnConnectUri;

    /**
     * The LIVY Spark batch job ID got from job submission
     */
    private int batchId;

    /**
     * The Spark Batch Job submission parameter
     */
    protected SparkSubmissionParameter submissionParameter;

    /**
     * The Spark Batch Job submission for RestAPI transaction
     */
    private SparkBatchSubmission submission;

    /**
     * The setting of maximum retry count in RestAPI calling
     */
    private int retriesMax = 3;

    /**
     * The setting of delay seconds between tries in RestAPI calling
     */
    private int delaySeconds = 10;

    /**
     * The global cache for fetched Yarn UI page by browser
     */
    private Cache globalCache = new Cache();

    /**
     * The driver log conversion mode
     */
//    @Nullable
//    private DriverLogConversionMode driverLogConversionMode = null;

//    @Nullable
//    private IHDIStorageAccount storageAccount;

    @NonNull
    private LivyCluster cluster;
    /**
     * Access token used for uploading files to ADLS storage account
     */
    @Nullable
    private String accessToken;

    @Nullable
    private String destinationRootPath;

    public LivySparkBatch(
            LivyCluster cluster,
            SparkSubmissionParameter submissionParameter,
            SparkBatchSubmission sparkBatchSubmission,
            Observer<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject) {
        this(cluster, submissionParameter, sparkBatchSubmission, ctrlSubject, null, null);
    }


    public LivySparkBatch(
            LivyCluster cluster,
            SparkSubmissionParameter submissionParameter,
            SparkBatchSubmission sparkBatchSubmission,
            Observer<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject,
//            @Nullable IHDIStorageAccount storageAccount,
            @Nullable String accessToken,
            @Nullable String destinationRootPath) {
        this.cluster = cluster;
        this.submissionParameter = submissionParameter;
//        this.storageAccount = storageAccount;
        this.submission = sparkBatchSubmission;
        this.ctrlSubject = ctrlSubject;
        this.accessToken = accessToken;
        this.destinationRootPath = destinationRootPath;

//        tryInitAuthInfo();
    }

//    private void tryInitAuthInfo() {
//        try {
//            IClusterDetail clusterDetail = getCluster();
//
//             if (!StringUtils.isEmpty(clusterDetail.getHttpUserName()) && !StringUtils.isEmpty(clusterDetail.getHttpPassword())) {
//                this.submission.setUsernamePasswordCredential(clusterDetail.getHttpUserName(), clusterDetail.getHttpPassword());
//             }
//        } catch (Exception ex) {
//             log().warn("try to set authorization info fail: " + ex.toString());
//        }
//    }

    /**
     * Getter of Spark Batch Job submission parameter
     *
     * @return the instance of Spark Batch Job submission parameter
     */
    public SparkSubmissionParameter getSubmissionParameter() {
        return submissionParameter;
    }

    /**
     * Getter of the Spark Batch Job submission for RestAPI transaction
     *
     * @return the Spark Batch Job submission
     */
    public SparkBatchSubmission getSubmission() {
        return submission;
    }

    /**
     * Getter of the base connection URI for HDInsight Spark Job service
     *
     * @return the base connection URI for HDInsight Spark Job service
     */
    @Override
    public URI getConnectUri() {
//        if (connectUri == null) {
//            try {
//                SparkBatchSubmission.getInstance()
//                        .setUsernamePasswordCredential(getCluster().getHttpUserName(), getCluster().getHttpPassword());
//            } catch (HDIException e) {
//                log().warn("No credential provided for Spark batch job.");
//            }
//
//            this.connectUri = Optional.of(getCluster())
//                    .filter(c -> c instanceof LivyCluster)
//                    .map(c -> ((LivyCluster) c).getLivyBatchUrl())
//                    .map(URI::create)
//                    .orElse(null);
//        }
        return URI.create(getCluster().getLivyBatchUrl());
    }

    @NonNull
    public LivyCluster getCluster() {
        return cluster;
    }

//    @Nullable
//    public IHDIStorageAccount getStorageAccount() {
//        return storageAccount;
//    }

    /**
     * Getter of the LIVY Spark batch job ID got from job submission
     *
     * @return the LIVY Spark batch job ID
     */
    @Override
    public int getBatchId() {
        return batchId;
    }

    /**
     * Setter of LIVY Spark batch job ID got from job submission
     *
     * @param batchId the LIVY Spark batch job ID
     */
    protected void setBatchId(int batchId) {
        this.batchId = batchId;
    }

    /**
     * Getter of the maximum retry count in RestAPI calling
     *
     * @return the maximum retry count in RestAPI calling
     */
    @Override
    public int getRetriesMax() {
        return retriesMax;
    }

    /**
     * Setter of the maximum retry count in RestAPI calling
     * @param retriesMax the maximum retry count in RestAPI calling
     */
    @Override
    public void setRetriesMax(int retriesMax) {
        this.retriesMax = retriesMax;
    }

    /**
     * Getter of the delay seconds between tries in RestAPI calling
     *
     * @return the delay seconds between tries in RestAPI calling
     */
    @Override
    public int getDelaySeconds() {
        return delaySeconds;
    }

    /**
     * Setter of the delay seconds between tries in RestAPI calling
     * @param delaySeconds the delay seconds between tries in RestAPI calling
     */
    @Override
    public void setDelaySeconds(int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    /**
     * Create a batch Spark job
     *
     * @return the current instance for chain calling
     * @throws IOException the exceptions for networking connection issues related
     */
    private LivySparkBatch createBatchJob()
            throws IOException {
        // Submit the batch job
        HttpResponse httpResponse = this.getSubmission().createBatchSparkJob(
                getConnectUri().toString(), this.getSubmissionParameter());

        // Get the batch ID from response and save it
        if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
            SparkSubmitResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                    httpResponse.getMessage(), SparkSubmitResponse.class)
                    .orElseThrow(() -> new UnknownServiceException(
                            "Bad spark job response: " + httpResponse.getMessage()));

            this.setBatchId(jobResp.getId());

            return this;
        }

        throw new UnknownServiceException(String.format(
                "Failed to submit Spark batch job. error code: %d, type: %s, reason: %s.",
                httpResponse.getCode(), httpResponse.getContent(), httpResponse.getMessage()));
    }

    /**
     * Kill the batch job specified by ID
     *
     * @return the current instance for chain calling
     */
    @Override
    public Observable<? extends SparkBatchJob> killBatchJob() {
        return Observable.fromCallable(() -> {
            HttpResponse deleteResponse = this.getSubmission().killBatchJob(
                    this.getConnectUri().toString(), this.getBatchId());

            if (deleteResponse.getCode() > 300) {
                throw new UnknownServiceException(String.format(
                        "Failed to stop spark job. error code: %d, reason: %s.",
                        deleteResponse.getCode(), deleteResponse.getContent()));
            }

            return this;
        });
    }

//    @Override
//    public Observable<SimpleImmutableEntry<String, Long>> getDriverLog(@NonNull String type, long logOffset, int size) {
//        return getSparkJobDriverLogUrlObservable()
//                .flatMap(logUrl -> {
//                    long offset = logOffset;
//
//                    if (!StringUtils.equals(logUrl, getCurrentLogUrl())) {
//                        setCurrentLogUrl(logUrl);
//                        offset = 0;
//                    }
//
//                    if (getCurrentLogUrl() == null) {
//                        return Observable.empty();
//                    }
//
//                    String logGot = getInformationFromYarnLogDom(
//                            getSubmission().getCredentialsProvider(),
//                            getCurrentLogUrl(),
//                            type,
//                            offset,
//                            size);
//
//                    if (StringUtils.isEmpty(logGot)) {
//                        return Observable.empty();
//                    }
//
//                    return Observable.just(new SimpleImmutableEntry<>(logGot, offset));
//                });
//    }

//    private String getInformationFromYarnLogDom(final CredentialsProvider credentialsProvider,
//                                                      @NonNull String baseUrl,
//                                                      @NonNull String type,
//                                                      long start,
//                                                      int size) {
//        final WebClient HTTP_WEB_CLIENT = new WebClient(BrowserVersion.CHROME);
////        HTTP_WEB_CLIENT.getOptions().setUseInsecureSSL(HttpObservable.isSSLCertificateValidationDisabled());
//        HTTP_WEB_CLIENT.setCache(globalCache);
//
//        if (credentialsProvider != null) {
//            HTTP_WEB_CLIENT.setCredentialsProvider(credentialsProvider);
//        }
//
//        URI url = null;
//
//        try {
//            url = new URI(baseUrl + "/").resolve(
//                    String.format("%s?start=%d", type, start) +
//                            (size <= 0 ? "" : String.format("&&end=%d", start + size)));
//            HtmlPage htmlPage = HTTP_WEB_CLIENT.getPage(url.toString());
//
//            Iterator<DomElement> iterator = htmlPage.getElementById("navcell").getNextElementSibling().getChildElements().iterator();
//
//            HashMap<String, String> logTypeMap = new HashMap<>();
//            final AtomicReference<String> logType = new AtomicReference<>();
//            String logs = "";
//
//            while (iterator.hasNext()) {
//                DomElement node = iterator.next();
//
//                if (node instanceof HtmlParagraph) {
//                    // In history server, need to read log type paragraph in page
//                    final Pattern logTypePattern = Pattern.compile("Log Type:\\s+(\\S+)");
//
//                    Optional.ofNullable(node.getFirstChild())
//                            .map(DomNode::getTextContent)
//                            .map(String::trim)
//                            .map(logTypePattern::matcher)
//                            .filter(Matcher::matches)
//                            .map(matcher -> matcher.group(1))
//                            .ifPresent(logType::set);
//                } else if (node instanceof HtmlPreformattedText) {
//                    // In running, no log type paragraph in page
//                    logs = Optional.ofNullable(node.getFirstChild())
//                            .map(DomNode::getTextContent)
//                            .orElse("");
//
//                    if (logType.get() != null) {
//                        // Only get the first <pre>...</pre>
//                        logTypeMap.put(logType.get(), logs);
//
//                        logType.set(null);
//                    }
//                }
//            }
//
//            return logTypeMap.getOrDefault(type, logs);
//        } catch (FailingHttpStatusCodeException httpError) {
//            // If the URL is wrong, will get 200 response with content:
//            //      Unable to locate 'xxx' log for container
//            //  OR
//            //      Logs not available for <user>. Aggregation may not be complete, Check back later or try the nodemanager at...
//            //  OR
//            //      Cannot get container logs without ...
//            //
//            // if fetching Yarn log hits the gap between the job running and stop, will get the status 403
//            // the log is moving to job history server, just wait and retry.
//            if (httpError.getStatusCode() != HttpStatus.SC_FORBIDDEN) {
//                getCtrlSubject().onNext(new SimpleImmutableEntry<>(
//                        Warning, "The GET request to " + url + " responded error: " + httpError.getMessage()));
//            }
//        } catch (URISyntaxException e) {
//            getCtrlSubject().onNext(new SimpleImmutableEntry<>(
//                    Error, "baseUrl has syntax error: " + baseUrl));
//        } catch (Exception e) {
//            getCtrlSubject().onNext(new SimpleImmutableEntry<>(
//                    Warning, "get Spark job log Error" + e.getMessage()));
//        }
//        return "";
//    }
//    /**
//     * Parse host from host:port combination string
//     *
//     * @param driverHttpAddress the host:port combination string to parse
//     * @return the host got, otherwise null
//     */
//    String parseAmHostHttpAddressHost(@Nullable String driverHttpAddress) {
//        if (driverHttpAddress == null) {
//            return null;
//        }
//
//        Pattern driverRegex = Pattern.compile("(?<host>[^:]+):(?<port>\\d+)");
//        Matcher driverMatcher = driverRegex.matcher(driverHttpAddress);
//
//        return driverMatcher.matches() ? driverMatcher.group("host") : null;
//    }

    /**
     * Get Spark Job Yarn application state with retries
     *
     * @return the Yarn application state got
     * @throws IOException exceptions in transaction
     */
    public String getState() throws IOException {
        int retries = 0;

        do {
            try {
                HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                        this.getConnectUri().toString(), batchId);

                if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                    SparkSubmitResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                            httpResponse.getMessage(), SparkSubmitResponse.class)
                            .orElseThrow(() -> new UnknownServiceException(
                                    "Bad spark job response: " + httpResponse.getMessage()));

                    return jobResp.getState();
                }
            } catch (IOException e) {
                log().debug("Got exception " + e.toString() + ", waiting for a while to try", e);
            }

            try {
                // Retry interval
                sleep(TimeUnit.SECONDS.toMillis(this.getDelaySeconds()));
            } catch (InterruptedException ex) {
                throw new IOException("Interrupted in retry attempting", ex);
            }
        } while (++retries < this.getRetriesMax());

        throw new UnknownServiceException("Failed to get job state: Unknown service error after " + --retries + " retries");
    }

    /**
     * Get Spark Job Yarn application ID with retries
     *
     * @param batchBaseUri the connection URI
     * @param batchId the Livy batch job ID
     * @return the Yarn application ID got
     * @throws IOException exceptions in transaction
     */
    String getSparkJobApplicationId(URI batchBaseUri, int batchId) throws IOException {
        int retries = 0;

        do {
            try {
                HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                        batchBaseUri.toString(), batchId);

                if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                    SparkSubmitResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                            httpResponse.getMessage(), SparkSubmitResponse.class)
                            .orElseThrow(() -> new UnknownServiceException(
                                    "Bad spark job response: " + httpResponse.getMessage()));

                    if (jobResp.getAppId() != null) {
                        return jobResp.getAppId();
                    }
                }
            } catch (IOException e) {
                log().debug("Got exception " + e.toString() + ", waiting for a while to try", e);
            }

            try {
                // Retry interval
                sleep(TimeUnit.SECONDS.toMillis(this.getDelaySeconds()));
            } catch (InterruptedException ex) {
                throw new IOException("Interrupted in retry attempting", ex);
            }
        } while (++retries < this.getRetriesMax());

        throw new UnknownServiceException("Failed to get job Application ID: Unknown service error after " + --retries + " retries");
    }

//    /**
//     * Get Spark Job Yarn application with retries
//     *
//     * @param applicationID the Yarn application ID
//     * @return the Yarn application got
//     * @throws IOException exceptions in transaction
//     */
//    private App getSparkJobYarnApplication(URI yarnConnectUri, String applicationID) throws Exception {
//        if (yarnConnectUri == null) {
//            return null;
//        }
//
//        int retries = 0;
//
//        do {
//            // TODO: An issue here when the yarnui not sharing root with Livy batch job URI
//            URI getYarnClusterAppURI = URI.create(yarnConnectUri.toString() + applicationID);
//
//            try {
//                HttpResponse httpResponse = this.getSubmission()
//                        .getHttpResponseViaGet(getYarnClusterAppURI.toString());
//
//                if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
//                    Optional<AppResponse> appResponse = ObjectConvertUtils.convertJsonToObject(
//                            httpResponse.getMessage(), AppResponse.class);
//                    return appResponse
//                            .orElseThrow(() -> new UnknownServiceException(
//                                    "Bad response when getting from " + getYarnClusterAppURI + ", " +
//                                            "response " + httpResponse.getMessage()))
//                            .getApp();
//                }
//            } catch (IOException e) {
//                log().debug("Got exception " + e.toString() + ", waiting for a while to try", e);
//            }
//
//            try {
//                // Retry interval
//                sleep(TimeUnit.SECONDS.toMillis(this.getDelaySeconds()));
//            } catch (InterruptedException ex) {
//                throw new IOException("Interrupted in retry attempting", ex);
//            }
//        } while (++retries < this.getRetriesMax());
//
//        throw new UnknownServiceException("Failed to get job Yarn application: Unknown service error after " + --retries + " retries");
//    }

    /**
     * New RxAPI: Get current job application Id
     *
     * @return Application Id Observable
     */
    Observable<String> getSparkJobApplicationIdObservable() {
        return Observable.fromCallable(() -> {
            HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                    getConnectUri().toString(), getBatchId());

            if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                SparkSubmitResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                        httpResponse.getMessage(), SparkSubmitResponse.class)
                        .orElseThrow(() -> new UnknownServiceException(
                                "Bad spark job response: " + httpResponse.getMessage()));

                return jobResp.getAppId();
            }

            throw new UnknownServiceException("Can't get Spark Application Id");
        });
    }


    /**
     * New RxAPI: Get the current Spark job Yarn application attempt containers
     *
     * @return The string pair Observable of Host and Container Id
     */
    Observable<SimpleImmutableEntry<URI, String>> getSparkJobYarnContainersObservable(@NonNull AppAttempt appAttempt) {
        return loadPageByBrowserObservable(getConnectUri().resolve("/yarnui/hn/cluster/appattempt/")
                                                          .resolve(appAttempt.getAppAttemptId()).toString())
                .retry(getRetriesMax())
                .repeatWhen(ob -> ob.delay(getDelaySeconds(), TimeUnit.SECONDS))
                .filter(this::isSparkJobYarnAppAttemptNotJustLaunched)
                .map(htmlPage -> htmlPage.getFirstByXPath("//*[@id=\"containers\"]/tbody")) // Get the container table by XPath
                .filter(Objects::nonNull)       // May get null in the last step
                .map(HtmlTableBody.class::cast)
                .map(HtmlTableBody::getRows)    // To container rows
                .buffer(2, 1)
                // Wait for last two refreshes getting the same rows count, which means the yarn application
                // launching containers finished
                .takeUntil(buf -> buf.size() == 2 && buf.get(0).size() == buf.get(1).size())
                .filter(buf -> buf.size() == 2 && buf.get(0).size() == buf.get(1).size())
                .map(buf -> buf.get(1))
                .flatMap(Observable::from)  // From rows to row one by one
                .filter(containerRow -> {
                    try {
                        // Read container URL from YarnUI page
                        String urlFromPage = ((HtmlAnchor) containerRow.getCell(3).getFirstChild()).getHrefAttribute();
                        URI containerUri = getConnectUri().resolve(urlFromPage);

                        return loadPageByBrowserObservable(containerUri.toString())
                                .map(this::isSparkJobYarnContainerLogAvailable)
                                .toBlocking()
                                .singleOrDefault(false);
                    } catch (Exception ignore) {
                        return false;
                    }
                })
                .map(row -> {
                    URI hostUrl = URI.create(row.getCell(1).getTextContent().trim());
                    String containerId = row.getCell(0).getTextContent().trim();

                    return new SimpleImmutableEntry<>(hostUrl, containerId);
                });
    }

    /*
     * Parsing the Application Attempt HTML page to determine if the attempt is running
     */
    private Boolean isSparkJobYarnAppAttemptNotJustLaunched(@NonNull HtmlPage htmlPage) {
        // Get the info table by XPath
        @Nullable
        HtmlTableBody infoBody = htmlPage.getFirstByXPath("//*[@class=\"info\"]/tbody");

        if (infoBody == null) {
            return false;
        }

        return infoBody
                .getRows()
                .stream()
                .filter(row -> row.getCells().size() >= 2)
                .filter(row -> row.getCell(0)
                                  .getTextContent()
                                  .trim()
                                  .toLowerCase()
                                  .equals("application attempt state:"))
                .map(row -> !row.getCell(1)
                                .getTextContent()
                                .trim()
                                .toLowerCase()
                                .equals("launched"))
                .findFirst()
                .orElse(false);
    }

    private Boolean isSparkJobYarnContainerLogAvailable(@NonNull HtmlPage htmlPage) {
        Optional<DomElement> firstContent = Optional.ofNullable(
                htmlPage.getFirstByXPath("//*[@id=\"layout\"]/tbody/tr/td[2]"));

        return firstContent.map(DomElement::getTextContent)
                           .map(line -> !line.trim()
                                            .toLowerCase()
                                            .contains("no logs available"))
                           .orElse(false);
    }

    private Observable<HtmlPage> loadPageByBrowserObservable(String url) {
        final WebClient HTTP_WEB_CLIENT = new WebClient(BrowserVersion.CHROME);
        HTTP_WEB_CLIENT.setCache(globalCache);

        if (getSubmission().getCredentialsProvider() != null) {
            HTTP_WEB_CLIENT.setCredentialsProvider(getSubmission().getCredentialsProvider());
        }

        return Observable.create(ob -> {
            try {
                ob.onNext(HTTP_WEB_CLIENT.getPage(url));
                ob.onCompleted();
            } catch (ScriptException e) {
                log().debug("get Spark job Yarn attempts detail browser rendering Error", e);
            } catch (IOException e) {
                ob.onError(e);
            }
        });
    }

    /**
     * Get Spark Job driver log URL with retries
     *
     * @deprecated
     * The Livy Rest API driver log Url field only get the running job.
     * Use getSparkJobDriverLogUrlObservable() please, with RxJava supported.
     *
     * @param batchBaseUri the connection URI
     * @param batchId the Livy batch job ID
     * @return the Spark Job driver log URL
     * @throws IOException exceptions in transaction
     */
    @Nullable
    @Deprecated
    public String getSparkJobDriverLogUrl(URI batchBaseUri, int batchId) throws IOException {
        int retries = 0;

        do {
            HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                    batchBaseUri.toString(), batchId);

            try {
                if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                    SparkSubmitResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                            httpResponse.getMessage(), SparkSubmitResponse.class)
                            .orElseThrow(() -> new UnknownServiceException(
                                    "Bad spark job response: " + httpResponse.getMessage()));

                    String driverLogUrl = (String) jobResp.getAppInfo().get("driverLogUrl");

                    if (jobResp.getAppId() != null && driverLogUrl != null) {
                        return driverLogUrl;
                    }
                }
            } catch (IOException e) {
                log().debug("Got exception " + e.toString() + ", waiting for a while to try", e);
            }


            try {
                // Retry interval
                sleep(TimeUnit.SECONDS.toMillis(this.getDelaySeconds()));
            } catch (InterruptedException ex) {
                throw new IOException("Interrupted in retry attempting", ex);
            }
        } while (++retries < this.getRetriesMax());

        throw new UnknownServiceException("Failed to get job driver log URL: Unknown service error after " + --retries + " retries");
    }

//    /**
//     * Get Spark batch job driver host by ID
//     *
//     * @return Spark driver node host observable
//     */
//    @Override
//    public Observable<String> getSparkDriverHost() {
//        return Observable.fromCallable(() -> {
//            String applicationId = this.getSparkJobApplicationId(this.getConnectUri(), this.getBatchId());
//
//            App yarnApp = this.getSparkJobYarnApplication(this.getYarnNMConnectUri(), applicationId);
//
//            if (yarnApp == null) {
//                throw new Exception("Can not access yarn applicaition since yarnConnectUri is null");
//            }
//
//            if (yarnApp.isFinished()) {
//                throw new UnknownServiceException("The Livy job " + this.getBatchId() + " on yarn is not running.");
//            }
//
//            String driverHttpAddress = yarnApp.getAmHostHttpAddress();
//
//            /*
//             * The sample here is:
//             *     host.domain.com:8900
//             *       or
//             *     10.0.0.15:30060
//             */
//            String driverHost = this.parseAmHostHttpAddressHost(driverHttpAddress);
//
//            if (driverHost == null) {
//                throw new UnknownServiceException(
//                        "Bad amHostHttpAddress got from /yarnui/ws/v1/cluster/apps/" + applicationId);
//            }
//
//            return driverHost;
//        });
//    }

    @Override
    @NonNull
    public Observable<SimpleImmutableEntry<MessageInfoType, String>> getSubmissionLog() {
        // Those lines are carried per response,
        // if there is no value followed, the line should not be sent to console
        final Set<String> ignoredEmptyLines = new HashSet<>(Arrays.asList(
                "stdout:",
                "stderr:",
                "yarn diagnostics:"));

        return Observable.create(ob -> {
            try {
                int start = 0;
                final int maxLinesPerGet = 128;
                int linesGot;
                boolean isSubmitting = true;

                while (isSubmitting) {
                    Boolean isAppIdAllocated = !this.getSparkJobApplicationIdObservable().isEmpty().toBlocking().lastOrDefault(true);
                    String logUrl = String.format("%s/%d/log?from=%d&size=%d",
                            this.getConnectUri().toString(), batchId, start, maxLinesPerGet);

                    HttpResponse httpResponse = this.getSubmission().getHttpResponseViaGet(logUrl);

                    SparkJobLog sparkJobLog = ObjectConvertUtils.convertJsonToObject(httpResponse.getMessage(),
                            SparkJobLog.class)
                            .orElseThrow(() -> new UnknownServiceException(
                                    "Bad spark log response: " + httpResponse.getMessage()));

                    // To subscriber
                    sparkJobLog.getLog().stream()
                            .filter(line -> !ignoredEmptyLines.contains(line.trim().toLowerCase()))
                            .forEach(line -> ob.onNext(new SimpleImmutableEntry<>(Log, line)));

                    linesGot = sparkJobLog.getLog().size();
                    start += linesGot;

                    // Retry interval
                    if (linesGot == 0) {
                        isSubmitting = this.getState().equals("starting") && !isAppIdAllocated;

                        sleep(TimeUnit.SECONDS.toMillis(this.getDelaySeconds()));
                    }
                }
            } catch (IOException ex) {
                ob.onNext(new SimpleImmutableEntry<>(MessageInfoType.Error, ex.getMessage()));
            } catch (InterruptedException ignored) {
            } finally {
                ob.onCompleted();
            }
        });
    }

    public boolean isActive() throws IOException {
        int retries = 0;

        do {
            try {
                HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                        this.getConnectUri().toString(), batchId);

                if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                    SparkSubmitResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                            httpResponse.getMessage(), SparkSubmitResponse.class)
                            .orElseThrow(() -> new UnknownServiceException(
                                    "Bad spark job response: " + httpResponse.getMessage()));

                    return jobResp.isAlive();
                }
            } catch (IOException e) {
                log().debug("Got exception " + e.toString() + ", waiting for a while to try", e);
            }

            try {
                // Retry interval
                sleep(TimeUnit.SECONDS.toMillis(this.getDelaySeconds()));
            } catch (InterruptedException ex) {
                throw new IOException("Interrupted in retry attempting", ex);
            }
        } while (++retries < this.getRetriesMax());

        throw new UnknownServiceException("Failed to detect job activity: Unknown service error after " + --retries + " retries");
    }

    protected Observable<SimpleImmutableEntry<String, String>> getJobDoneObservable() {
        return Observable.create((Subscriber<? super SimpleImmutableEntry<String, String>> ob) -> {
            try {
                boolean isJobActive;
                SparkBatchJobState state = SparkBatchJobState.NOT_STARTED;
                String diagnostics = "";

                do {
                    HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                            this.getConnectUri().toString(), batchId);

                    if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                        SparkSubmitResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                                httpResponse.getMessage(), SparkSubmitResponse.class)
                                .orElseThrow(() -> new UnknownServiceException(
                                        "Bad spark job response: " + httpResponse.getMessage()));

                        state = SparkBatchJobState.valueOf(jobResp.getState().toUpperCase());
                        diagnostics = String.join("\n", jobResp.getLog());

                        isJobActive = !isDone(state.toString());
                    } else {
                        isJobActive = false;
                    }


                    // Retry interval
                    sleep(1000);
                } while (isJobActive);

                ob.onNext(new SimpleImmutableEntry<>(state.toString(), diagnostics));
                ob.onCompleted();
            } catch (IOException ex) {
                ob.onError(ex);
            } catch (InterruptedException ignored) {
                ob.onCompleted();
            }
        });
    }
//
//    protected Observable<String> getJobLogAggregationDoneObservable() {
//        return getSparkJobApplicationIdObservable()
//                .flatMap(applicationId ->
//                        Observable.fromCallable(() ->
//                                getSparkJobYarnApplication(this.getYarnNMConnectUri(), applicationId))
//                                .repeatWhen(ob -> ob.delay(getDelaySeconds(), TimeUnit.SECONDS))
//                                .filter(app -> app != null)
//                                .takeUntil(this::isYarnAppLogAggregationDone)
//                                .filter(this::isYarnAppLogAggregationDone))
//                .map(yarnApp -> yarnApp.getLogAggregationStatus().toUpperCase());
//    }

//    private Boolean isYarnAppLogAggregationDone(App yarnApp) {
//        switch (yarnApp.getLogAggregationStatus().toUpperCase()) {
//            case "SUCCEEDED":
//            case "FAILED":
//            case "TIME_OUT":
//                return true;
//            case "DISABLED":
//            case "NOT_START":
//            case "RUNNING":
//            case "RUNNING_WITH_FAILURE":
//            default:
//                return false;
//        }
//    }

//    /**
//     * New RxAPI: Get Job Driver Log URL from the container
//     *
//     * @return Job Driver log URL observable
//     */
//    Observable<String> getSparkJobDriverLogUrlObservable() {
//        return getSparkJobYarnCurrentAppAttempt()
//                .map(AppAttempt::getLogsLink)
//                .map(URI::create)
//                .filter(uri -> StringUtils.isNotEmpty(uri.getHost()))
//                .flatMap(this::convertToPublicLogUri)
//                .map(URI::toString);
//    }

//    boolean isUriValid(@NonNull URI uriProbe) throws IOException {
//        return getSubmission().getHttpResponseViaGet(uriProbe.toString()).getCode() < 300;
//    }
//
//    private Optional<URI> convertToPublicLogUri(@Nullable DriverLogConversionMode mode, @NonNull URI internalLogUrl) {
//        String normalizedPath = Optional.of(internalLogUrl.getPath()).filter(StringUtils::isNotBlank).orElse("/");
//
//        if (mode != null) {
//            switch (mode) {
//                case WITHOUT_PORT:
//                    return Optional.of(getConnectUri().resolve(
//                            String.format("/yarnui/%s%s", internalLogUrl.getHost(), normalizedPath)));
//                case WITH_PORT:
//                    return Optional.of(getConnectUri().resolve(
//                            String.format("/yarnui/%s/port/%s%s",
//                                    internalLogUrl.getHost(),
//                                    internalLogUrl.getPort(),
//                                    normalizedPath)));
//                case ORIGINAL:
//                    return Optional.of(internalLogUrl);
//            }
//        }
//
//        return Optional.empty();
//    }
//
//    public Observable<URI> convertToPublicLogUri(@NonNull URI internalLogUri) {
//        // New version, without port info in log URL
//        return convertToPublicLogUri(getLogUriConversionMode(), internalLogUri)
//                .map(Observable::just)
//                .orElseGet(() -> {
//                    // Probe usable driver log URI
//                    DriverLogConversionMode probeMode = getLogUriConversionMode();
//
//                    while ((probeMode = DriverLogConversionMode.next(probeMode)) != null) {
//                        Optional<URI> uri = convertToPublicLogUri(probeMode, internalLogUri)
//                                .filter(uriProbe -> {
//                                    try {
//                                        return isUriValid(uriProbe);
//                                    } catch (IOException e) {
//                                        return false;
//                                    }
//                                });
//
//                        if (uri.isPresent()) {
//                            // Find usable one
//                            setDriverLogConversionMode(probeMode);
//
//                            return Observable.just(uri.get());
//                        }
//                    }
//
//                    // All modes were probed and all failed
//                    return Observable.empty();
//                });
//    }

//    @Nullable
//    private DriverLogConversionMode getLogUriConversionMode() {
//        return this.driverLogConversionMode;
//    }
//
//    private void setDriverLogConversionMode(@Nullable DriverLogConversionMode driverLogConversionMode) {
//        this.driverLogConversionMode = driverLogConversionMode;
//    }

    @NonNull
    @Override
    public Observer<SimpleImmutableEntry<MessageInfoType, String>> getCtrlSubject() {
        return ctrlSubject;
    }

    @NonNull
    @Override
    public Observable<? extends SparkBatchJob> deploy(@NonNull String artifactPath) {
        return Observable.error(new UnsupportedOperationException());
    }

    /**
     * New RxAPI: Submit the job
     *
     * @return Spark Job observable
     */
    @Override
    @NonNull
    public Observable<? extends SparkBatchJob> submit() {
        return Observable.fromCallable(() -> {
            return createBatchJob();
        });
    }




    @Override
    public boolean isDone(@NonNull String state) {
        switch (SparkBatchJobState.valueOf(state.toUpperCase())) {
            case SHUTTING_DOWN:
            case ERROR:
            case DEAD:
            case SUCCESS:
                return true;
            case NOT_STARTED:
            case STARTING:
            case RUNNING:
            case RECOVERING:
            case BUSY:
            case IDLE:
            default:
                return false;
        }
    }

    @Override
    public boolean isRunning(@NonNull String state) {
        return SparkBatchJobState.valueOf(state.toUpperCase()) == SparkBatchJobState.RUNNING;
    }

    @Override
    public boolean isSuccess(@NonNull String state) {
        return SparkBatchJobState.valueOf(state.toUpperCase()) == SparkBatchJobState.SUCCESS;
    }

    /**
     * New RxAPI: Get the job status (from livy)
     *
     * @return Spark Job observable
     */
    @NonNull
    public Observable<? extends SparkSubmitResponse> getStatus() {
        return Observable.fromCallable(() -> {
            HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                    this.getConnectUri().toString(), getBatchId());

            if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                return ObjectConvertUtils.convertJsonToObject(
                        httpResponse.getMessage(), SparkSubmitResponse.class)
                        .orElseThrow(() -> new UnknownServiceException(
                                "Bad spark job response: " + httpResponse.getMessage()));
            }

            throw new SparkJobException("Can't get cluster " + getSubmissionParameter().getClusterName() + " status.");
        });
    }

    @NonNull
    @Override
    public Observable<String> awaitStarted() {
        return getStatus()
                .map(status -> new SimpleImmutableEntry<>(status.getState(), String.join("\n", status.getLog())))
                .retry(getRetriesMax())
                .repeatWhen(ob -> ob
                        .doOnNext(ignored -> {
                            getCtrlSubject().onNext(new SimpleImmutableEntry<>(Info, "The Spark job is starting..."));
                        })
                        .delay(getDelaySeconds(), TimeUnit.SECONDS)
                )
                .takeUntil(stateLogPair -> isDone(stateLogPair.getKey()) || isRunning(stateLogPair.getKey()))
                .filter(stateLogPair -> isDone(stateLogPair.getKey()) || isRunning(stateLogPair.getKey()))
                .flatMap(stateLogPair -> {
                    if (isDone(stateLogPair.getKey()) && !isSuccess(stateLogPair.getKey())) {
                        return Observable.error(
                                new SparkJobException("The Spark job failed to start due to " + stateLogPair.getValue()));
                    }

                    return Observable.just(stateLogPair.getKey());
                });
    }

    @NonNull
    @Override
    public Observable<SimpleImmutableEntry<String, String>> awaitDone() {
        return getJobDoneObservable();
    }

    @NonNull
    @Override
    public Observable<String> awaitPostDone() {
//        return getJobLogAggregationDoneObservable();
        return Observable.empty();
    }
}
