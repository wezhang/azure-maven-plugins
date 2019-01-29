package com.microsoft.azure.maven.spark.auth

import com.microsoft.azure.credentials.AzureCliCredentials
import com.microsoft.azure.credentials.MSICredentials
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.implementation.HDInsightManager
import com.microsoft.azure.maven.auth.AuthConfiguration
import com.microsoft.azure.maven.auth.AzureAuthHelper

class HDInsightAuthHelper(config: AuthConfiguration?) : AzureAuthHelper(config) {
    val log
        get() = config.log

    val hdInsightManager: HDInsightManager
        get() {
            val authTokenCredential = if (System.getenv(CLOUD_SHELL_ENV_KEY) != null) {
                log.info(AUTH_WITH_MSI)
                MSICredentials()
            } else {
                log.info(AUTH_WITH_AZURE_CLI)
                AzureCliCredentials.create()
            }

            return HDInsightManager.authenticate(authTokenCredential, authTokenCredential.defaultSubscriptionId())
        }
}