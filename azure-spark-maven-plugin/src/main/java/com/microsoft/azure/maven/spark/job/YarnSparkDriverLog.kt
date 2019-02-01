package com.microsoft.azure.maven.spark.job

import com.gargoylesoftware.htmlunit.BrowserVersion
import com.gargoylesoftware.htmlunit.Cache
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException
import com.gargoylesoftware.htmlunit.WebClient
import com.gargoylesoftware.htmlunit.html.HtmlPage
import com.gargoylesoftware.htmlunit.html.HtmlParagraph
import com.gargoylesoftware.htmlunit.html.HtmlPreformattedText
import com.microsoft.azure.maven.spark.clusters.YarnCluster
import com.microsoft.azure.maven.spark.restapi.ObjectConvertUtils
import com.microsoft.azure.maven.spark.restapi.SparkBatchSubmission
import com.microsoft.azure.maven.spark.restapi.yarn.rm.App
import com.microsoft.azure.maven.spark.restapi.yarn.rm.AppAttemptsResponse
import com.microsoft.azure.maven.spark.restapi.yarn.rm.AppResponse
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.CredentialsProvider
import rx.Observable
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.net.UnknownServiceException
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern

class YarnSparkApplicationDriverLog(val applicationId: String, val cluster: YarnCluster, private val submission: SparkBatchSubmission) : SparkDriverLog {
    override val yarnNMConnectUri: URI = URI.create(cluster.yarnNMConnectionUrl)

    var currentLogUrl: String? = null

    enum class DriverLogConversionMode {
        WITHOUT_PORT,
        WITH_PORT,
        ORIGINAL;


        companion object {
            fun next(current: DriverLogConversionMode?): DriverLogConversionMode? {
                val modes = Arrays.asList(*DriverLogConversionMode.values())

                if (current == null) {
                    return modes[0]
                }

                val found = modes.indexOf(current)

                return if (found + 1 >= modes.size) {
                    null
                } else modes[found + 1]

            }
        }
    }

    var logUriConversionMode: DriverLogConversionMode? = null

    /**
     * New RxAPI: Get the current Spark job Yarn application most recent attempt
     *
     * @return Yarn Application Attempt info Observable
     */
    private fun getSparkJobYarnCurrentAppAttemptLogsLink(appId: String): Observable<URI> {
        return Observable.fromCallable {
            val getYarnAppAttemptsURI = URI.create("$yarnNMConnectUri$appId/appattempts")

            val httpResponse = submission.getHttpResponseViaGet(getYarnAppAttemptsURI.toString())

            if (httpResponse.code in 200..299) {
                val appResponse = ObjectConvertUtils.convertJsonToObject(
                        httpResponse.message, AppAttemptsResponse::class.java)

                val currentAttempt = appResponse.map { it.appAttempts.appAttempt.maxBy { appAttempt -> appAttempt.id } }
                if (currentAttempt.isPresent) {
                    return@fromCallable currentAttempt.get()
                }
            }

            throw UnknownServiceException(
                    """Bad response when getting from $getYarnAppAttemptsURI, response ${httpResponse.message}""")
        }
        .map { URI.create(it.logsLink) }
    }

    private fun getSparkJobYarnApplication(): Observable<App> {
        return Observable.fromCallable {
            // TODO: An issue here when the yarnui not sharing root with Livy batch job URI
            val getYarnClusterAppURI = URI.create("$yarnNMConnectUri$applicationId")

            val httpResponse = submission.getHttpResponseViaGet(getYarnClusterAppURI.toString())

            if (httpResponse.code in 200..299) {
                val appResponse = ObjectConvertUtils.convertJsonToObject(httpResponse.message, AppResponse::class.java)

                if (appResponse.isPresent) {
                    return@fromCallable appResponse.get().app
                }
            }

            throw UnknownServiceException(
                    "Bad response when getting from $getYarnClusterAppURI, response ${httpResponse.message}")
        }
    }

    protected fun getJobLogAggregationDoneObservable(): Observable<String> {
        return getSparkJobYarnApplication()
                .repeatWhen { ob -> ob.delay(2, TimeUnit.SECONDS) }
                .filter { app -> app != null }
                .takeUntil { this.isYarnAppLogAggregationDone(it) }
                .filter { this.isYarnAppLogAggregationDone(it) }
                .map { yarnApp -> yarnApp.logAggregationStatus.toUpperCase() }
    }


    private fun isYarnAppLogAggregationDone(yarnApp: App): Boolean {
        return when (yarnApp.logAggregationStatus.toUpperCase()) {
            "SUCCEEDED", "FAILED", "TIME_OUT" -> true
            "DISABLED", "NOT_START", "RUNNING", "RUNNING_WITH_FAILURE" -> false
            else -> false
        }
    }
    /**
     * New RxAPI: Get Job Driver Log URL from the container
     *
     * @return Job Driver log URL observable
     */
    private fun getSparkJobDriverLogUrlObservable(): Observable<URI> {
        return getSparkJobYarnCurrentAppAttemptLogsLink(applicationId)
                .filter { uri -> !uri.host.isNullOrBlank() }
                .flatMap { this.convertToPublicLogUri(it) }
    }

    @Throws(IOException::class)
    internal fun isUriValid(uriProbe: URI): Observable<Boolean> {
        return Observable.fromCallable { submission.getHttpResponseViaGet(uriProbe.toString()).getCode() < 300 }
    }

    private fun convertToPublicLogUri(mode: DriverLogConversionMode?, internalLogUrl: URI): Optional<URI> {
        val normalizedPath = Optional.of(internalLogUrl.path).filter { !it.isNullOrBlank() }
                .orElse("/")
        val yarnUiBase = URI.create(cluster.yarnUIBaseUrl + if (cluster.yarnUIBaseUrl.endsWith("/")) "" else "/")

        return when (mode) {
            DriverLogConversionMode.WITHOUT_PORT -> Optional.of(yarnUiBase.resolve(
                    String.format("%s%s", internalLogUrl.host, normalizedPath)))
            DriverLogConversionMode.WITH_PORT -> Optional.of(yarnUiBase.resolve(
                    String.format("%s/port/%s%s", internalLogUrl.host, internalLogUrl.port, normalizedPath)))
            DriverLogConversionMode.ORIGINAL -> Optional.of(internalLogUrl)
            else -> Optional.empty()
        }
    }

    fun convertToPublicLogUri(internalLogUri: URI): Observable<URI> {
        // New version, without port info in log URL
        return convertToPublicLogUri(logUriConversionMode, internalLogUri)
                .map { Observable.just(it) }
                .orElseGet {
                    // Probe usable driver log URI
                    var probeMode = logUriConversionMode

                    while (probeMode != null) {
                        val uri = convertToPublicLogUri(probeMode, internalLogUri)
                                .filter { uriProbe ->
                                    try {
                                        isUriValid(uriProbe).toBlocking().singleOrDefault(false)
                                    } catch (e: IOException) {
                                        false
                                    }
                                }

                        if (uri.isPresent) {
                            // Find usable one
                            logUriConversionMode = probeMode

                            return@orElseGet Observable.just(uri.get())
                        }

                        probeMode = DriverLogConversionMode.next(probeMode)
                    }

                    // All modes were probed and all failed
                    Observable.empty()
                }
    }

    override fun getDriverLog(type: String, logOffset: Long, size: Int)
            : Observable<AbstractMap.SimpleImmutableEntry<String, Long>> {
        return getSparkJobDriverLogUrlObservable()
                .map { it.toString() }
                .flatMap { logUrl ->
                    var offset = logOffset

                    if (!StringUtils.equals(logUrl, currentLogUrl)) {
                        currentLogUrl = logUrl
                        offset = 0
                    }

                    var driverLogUrl = currentLogUrl
                            ?: return@flatMap Observable.empty<AbstractMap.SimpleImmutableEntry<String, Long>>()

                    val logGot = getInformationFromYarnLogDom(
                            submission.getCredentialsProvider(),
                            driverLogUrl,
                            type,
                            offset,
                            size)

                    if (StringUtils.isEmpty(logGot)) {
                        return@flatMap Observable.empty<AbstractMap.SimpleImmutableEntry<String, Long>>()
                    }

                    Observable.just(AbstractMap.SimpleImmutableEntry<String, Long>(logGot, offset))
                }
    }

    override val driverHost: Observable<String>
        get() = this.getSparkJobYarnApplication()
                .map { yarnApp ->


            if (yarnApp.isFinished) {
                throw UnknownServiceException("The Livy job $applicationId on yarn is not running.")
            }

            val driverHttpAddress = yarnApp.amHostHttpAddress

            /*
             * The sample here is:
             *     host.domain.com:8900
             *       or
             *     10.0.0.15:30060
             */
            val driverHost = this.parseAmHostHttpAddressHost(driverHttpAddress) ?: throw UnknownServiceException(
                    "Bad amHostHttpAddress got from /yarnui/ws/v1/cluster/apps/$applicationId")

            driverHost
        }

    /**
     * Parse host from host:port combination string
     *
     * @param driverHttpAddress the host:port combination string to parse
     * @return the host got, otherwise null
     */
    private fun parseAmHostHttpAddressHost(driverHttpAddress: String?): String? {
        if (driverHttpAddress == null) {
            return null
        }

        val driverRegex = Pattern.compile("(?<host>[^:]+):(?<port>\\d+)")
        val driverMatcher = driverRegex.matcher(driverHttpAddress)

        return if (driverMatcher.matches()) driverMatcher.group("host") else null
    }

    val globalCache = Cache()

    private fun getInformationFromYarnLogDom(credentialsProvider: CredentialsProvider?,
                                             baseUrl: String,
                                             type: String,
                                             start: Long,
                                             size: Int): String {
        val HTTP_WEB_CLIENT = WebClient(BrowserVersion.CHROME)
        //        HTTP_WEB_CLIENT.getOptions().setUseInsecureSSL(HttpObservable.isSSLCertificateValidationDisabled());
        HTTP_WEB_CLIENT.cache = globalCache

        if (credentialsProvider != null) {
            HTTP_WEB_CLIENT.credentialsProvider = credentialsProvider
        }

        var url: URI? = null

        try {
            url = URI("$baseUrl/").resolve(
                    String.format("%s?start=%d", type, start) + if (size <= 0) "" else String.format("&&end=%d", start + size))
            val htmlPage = HTTP_WEB_CLIENT.getPage<HtmlPage>(url!!.toString())

            val iterator = htmlPage.getElementById("navcell").nextElementSibling.childElements.iterator()

            val logTypeMap = HashMap<String, String>()
            val logType = AtomicReference<String>()
            var logs = ""

            while (iterator.hasNext()) {
                val node = iterator.next()

                if (node is HtmlParagraph) {
                    // In history server, need to read log type paragraph in page
                    val logTypePattern = Pattern.compile("Log Type:\\s+(\\S+)")

                    Optional.ofNullable(node.getFirstChild())
                            .map { it.getTextContent() }
                            .map { it.trimEnd() }
                            .map { logTypePattern.matcher(it) }
                            .filter { it.matches() }
                            .map { matcher -> matcher.group(1) }
                            .ifPresent { logType.set(it) }
                } else if (node is HtmlPreformattedText) {
                    // In running, no log type paragraph in page
                    logs = Optional.ofNullable(node.getFirstChild())
                            .map { it.textContent }
                            .orElse("")

                    if (logType.get() != null) {
                        // Only get the first <pre>...</pre>
                        logTypeMap[logType.get()] = logs

                        logType.set(null)
                    }
                }
            }

            return (logTypeMap as Map<String, String>).getOrDefault(type, logs)
        } catch (httpError: FailingHttpStatusCodeException) {
            // If the URL is wrong, will get 200 response with content:
            //      Unable to locate 'xxx' log for container
            //  OR
            //      Logs not available for <user>. Aggregation may not be complete, Check back later or try the nodemanager at...
            //  OR
            //      Cannot get container logs without ...
            //
            // if fetching Yarn log hits the gap between the job running and stop, will get the status 403
            // the log is moving to job history server, just wait and retry.
//            if (httpError.statusCode != HttpStatus.SC_FORBIDDEN) {
//                getCtrlSubject().onNext(AbstractMap.SimpleImmutableEntry<MessageInfoType, String>(
//                        Warning, "The GET request to " + url + " responded error: " + httpError.message))
//            }
        } catch (e: URISyntaxException) {
//            getCtrlSubject().onNext(AbstractMap.SimpleImmutableEntry<MessageInfoType, String>(
//                    Error, "baseUrl has syntax error: $baseUrl"))
        } catch (e: Exception) {
//            getCtrlSubject().onNext(AbstractMap.SimpleImmutableEntry<MessageInfoType, String>(
//                    Warning, "get Spark job log Error" + e.message))
        }

        return ""
    }
}