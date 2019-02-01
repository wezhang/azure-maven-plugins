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
import com.microsoft.azure.maven.spark.clusters.HdiClusterDetail
import com.microsoft.azure.maven.spark.events.MessageInfoType
import com.microsoft.azure.maven.spark.job.LivySparkBatch
import com.microsoft.azure.maven.spark.processes.SparkBatchJobRemoteProcess
import com.microsoft.azure.maven.spark.restapi.SparkBatchSubmission
import com.microsoft.azure.maven.spark.restapi.SparkSubmissionParameter
import com.microsoft.azure.maven.spark.ux.MavenScheduler
import com.microsoft.azure.storage.CloudStorageAccount
import org.apache.maven.plugin.MojoFailureException
import rx.subjects.PublishSubject
import java.net.URI
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

    @Parameter(property = "deploy.mainClass", required = true)
    var mainClass: String? = null

    override fun doExecute() {
        log.info("Uploading the artifact file to Azure Storage...")
        log.info("Working subscription: ${azureClient.currentSubscription.displayName()}")

        val cluster = hdInsightManager.clusters().list()
                .find { it.name() == destClusterName }
                ?: throw MojoFailureException("The destination cluster $destClusterName didn't be found")

        val coreSiteConfiguration = try {
            hdInsightManager.configurations().getAsync(cluster.resourceGroupName(), destClusterName, "core-site")
                    .toBlocking()
                    .singleOrDefault(null)
        } catch (err: Exception) {
            throw MojoFailureException("Can't get the cluster $destClusterName core site configuration", err)
        }

        val gatewayConfiguration = try {
            hdInsightManager.configurations().getAsync(cluster.resourceGroupName(), destClusterName, "gateway")
                    .toBlocking()
                    .singleOrDefault(null)
        } catch (err: Exception) {
            throw MojoFailureException("Can't get the cluster $destClusterName gateway configuration", err)
        }

        val defaultFS = coreSiteConfiguration["fs.defaultFS"] ?: throw MojoFailureException("No fs.defaultFS found")
        log.info("Cluster $destClusterName default storage is: $defaultFS")

        val storageAccountNamePattern = """^wasb[s]?://(.*)@([^.]*)\.blob\.(.*)$""".toRegex()
        val results = storageAccountNamePattern.matchEntire(defaultFS)?.groups ?: throw MojoFailureException("No storage account found")

        val storageContainerName = results[1]?.value ?: throw MojoFailureException("Parsed wrong storage account name")
        val storageAccountName = results[2]?.value ?: throw MojoFailureException("Parsed wrong storage blob")
        val endpointSuffix = results[3]?.value ?: throw MojoFailureException("Parsed wrong endpoint suffix")

        val storageKey = coreSiteConfiguration["fs.azure.account.key.$storageAccountName.blob.$endpointSuffix"]
                ?: throw MojoFailureException("No storage key found")

        val storageAccount = CloudStorageAccount.parse(
                "DefaultEndpointsProtocol=https;AccountName=$storageAccountName;AccountKey=$storageKey;EndpointSuffix=$endpointSuffix")
        log.info("Storage account $storageAccount")

        val artifactFile = project.artifact.file
        val uploadPath = "SparkSubmission/$datePathSegmentsWithUuid/${artifactFile.name}"
        log.info("Deploy $artifactFile to $defaultFS/$uploadPath")
        val packageUri = AzureStorageHelper.uploadFileAsBlob(
                artifactFile, storageAccount, storageContainerName, uploadPath)

        val innerPackageUri = URI.create(defaultFS.let {
            if (it.endsWith("/")) it else "$it/"
        }).resolve(uploadPath)

        log.info("successfully uploaded Spark Application artifact file $artifactFile to $packageUri")

        val hdiClusterDetail = HdiClusterDetail(cluster, coreSiteConfiguration, gatewayConfiguration)

        var httpUsername = hdiClusterDetail.httpUserName ?: throw MojoFailureException("No username found from gateway configuration")
        var httpPassword = hdiClusterDetail.httpPassword ?: throw MojoFailureException("No password found from gateway configuration")

        log.info("Use account $httpUsername to submit Spark application")

        val batchParam = SparkSubmissionParameter().apply {
            setClassName(mainClass)
            file = innerPackageUri.toString()
        }

        log.info("Spark application parameters: ${batchParam.serializeToJson()}")

        val submission = SparkBatchSubmission.getInstance().apply {
            setUsernamePasswordCredential(httpUsername, httpPassword)
        }

        val ctrlSubject = PublishSubject.create<AbstractMap.SimpleImmutableEntry<MessageInfoType, String>>()

        ctrlSubject.subscribe({ typedMessage ->
            when (typedMessage.key) {
                MessageInfoType.Error -> {
                    log.error(typedMessage.value)
                }
                MessageInfoType.Info, MessageInfoType.Log -> log.info(typedMessage.value)
                MessageInfoType.Warning -> log.warn(typedMessage.value)
                MessageInfoType.Hyperlink, MessageInfoType.HyperlinkWithText, null -> TODO()
            }
        }, { err ->
            throw MojoFailureException("Got error to submit Spark Application: ", err)
        })

        val sparkBatchJob = LivySparkBatch(hdiClusterDetail, batchParam, submission, ctrlSubject)

        val sparkBatchProcess = SparkBatchJobRemoteProcess(
                MavenScheduler, sparkBatchJob, innerPackageUri.toString(), "Submit Spark Application", ctrlSubject)

        sparkBatchProcess.start()
    }
}