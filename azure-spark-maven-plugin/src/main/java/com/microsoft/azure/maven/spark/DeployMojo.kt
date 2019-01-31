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
import com.microsoft.azure.storage.CloudStorageAccount
import org.apache.maven.plugin.MojoFailureException
import java.util.*


@Mojo(name = "deploy", defaultPhase = LifecyclePhase.DEPLOY)
class DeployMojo: AbstractAzureMojo() {
    val hdInsightAuthHelper = HDInsightAuthHelper(this)

    val hdInsightManager: HDInsightManager by lazy {
        log.info("Only supports $AUTH_WITH_AZURE_CLI in POC")
        hdInsightAuthHelper.hdInsightManager
    }

    val datePathSegmentsWithUuid: String
        get() {
            val year = Calendar.getInstance(TimeZone.getTimeZone("UTC")).get(Calendar.YEAR)
            val month = Calendar.getInstance(TimeZone.getTimeZone("UTC")).get(Calendar.MONTH) + 1
            val day = Calendar.getInstance(TimeZone.getTimeZone("UTC")).get(Calendar.DAY_OF_MONTH)

            val uniqueFolderId = UUID.randomUUID().toString()

            return String.format("%04d/%02d/%02d/%s", year, month, day, uniqueFolderId)
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

        val defaultFS = configuration["fs.defaultFS"] ?: throw MojoFailureException("No fs.defaultFS found")
        log.info("Cluster $destClusterName default storage is: $defaultFS")

        val storageAccountNamePattern = """^wasb[s]?://(.*)@([^.]*)\.blob\.(.*)$""".toRegex()
        val results = storageAccountNamePattern.matchEntire(defaultFS)?.groups ?: throw MojoFailureException("No storage account found")

        val storageContainerName = results[1]?.value ?: throw MojoFailureException("Parsed wrong storage account name")
        val storageAccountName = results[2]?.value ?: throw MojoFailureException("Parsed wrong storage blob")
        val endpointSuffix = results[3]?.value ?: throw MojoFailureException("Parsed wrong endpoint suffix")

        val storageKey = configuration["fs.azure.account.key.$storageAccountName.blob.$endpointSuffix"]
                ?: throw MojoFailureException("No storage key found")

        val storageAccount = CloudStorageAccount.parse(
                "DefaultEndpointsProtocol=https;AccountName=$storageAccountName;AccountKey=$storageKey;EndpointSuffix=$endpointSuffix")
        log.info("Storage account $storageAccount")

        val artifactFile = project.artifact.file
        val uploadPath = "SparkSubmission/$datePathSegmentsWithUuid/${artifactFile.name}"
        log.info("Deploy $artifactFile to $defaultFS/$uploadPath")
        val packageUri = AzureStorageHelper.uploadFileAsBlob(
                artifactFile, storageAccount, storageContainerName, uploadPath)

        log.info("successfully uploaded Spark Application artifact file $artifactFile to $packageUri")
    }
}