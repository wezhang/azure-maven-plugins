package com.microsoft.azure.maven.spark

/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

import com.microsoft.azure.maven.AbstractAzureMojo
import org.apache.maven.plugins.annotations.LifecyclePhase
import org.apache.maven.plugins.annotations.Mojo
import org.apache.maven.plugins.annotations.Parameter
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.implementation.HDInsightManager
import com.microsoft.azure.maven.auth.AzureAuthHelper.*
import com.microsoft.azure.maven.spark.auth.HDInsightAuthHelper
import org.apache.maven.plugin.MojoFailureException


@Mojo(name = "deploy", defaultPhase = LifecyclePhase.DEPLOY)
class DeployMojo: AbstractAzureMojo() {
    val hdInsightAuthHelper = HDInsightAuthHelper(this)

    val hdInsightManager: HDInsightManager by lazy {
        log.info("Only supports $AUTH_WITH_AZURE_CLI in POC")
        hdInsightAuthHelper.hdInsightManager
    }

    @Parameter(property = "deploy.dstClusterName", required = true)
    var destClusterName: String? = null

    override fun doExecute() {
        log.info("Uploading the artifact file to Azure Storage...")
        log.info("Working subscription: ${azureClient.currentSubscription.displayName()}")

//        log.info("Available HDInsight Clusters:")
        val cluster = hdInsightManager.clusters().list()
                .find { it.name() == destClusterName }
                ?: throw MojoFailureException("The destination cluster $destClusterName didn't be found")

        val configuration = try {
            hdInsightManager.configurations().getAsync(cluster.resourceGroupName(), destClusterName, "core-site")
                    .toBlocking()
                    .singleOrDefault(null)
        } catch (err: Exception) {
            throw MojoFailureException("Can't get the cluster $destClusterName configuration", err)
        }

        val defaultFS = configuration["fs.defaultFS"]
        val storageKey = configuration["fs.azure.account.key.storageaccount.blob.core.windows.net"]

        log.info("Cluster $destClusterName default storage is: $defaultFS")

//        val artifact: File
//        val connectionString: String
//        val storageAccount = CloudStorageAccount.parse(connectionString)
//        val packageUri = AzureStorageHelper.uploadFileAsBlob(
//                artifact, storageAccount, container, uploadedPath)
//        log.info("Successfully uploaded ZIP file to $packageUri")
    }
}