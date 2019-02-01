package com.microsoft.azure.maven.spark.job

import rx.Observable
import java.net.URI
import java.util.AbstractMap

interface SparkDriverLog {
    val yarnNMConnectUri: URI

    val driverHost: Observable<String>

    fun getDriverLog(type: String, logOffset: Long, size: Int): Observable<AbstractMap.SimpleImmutableEntry<String, Long>>
}