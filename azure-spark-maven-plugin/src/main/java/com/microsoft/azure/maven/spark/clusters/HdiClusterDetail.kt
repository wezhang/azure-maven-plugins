package com.microsoft.azure.maven.spark.clusters

import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.Cluster
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.Configurations

class HdiClusterDetail(cluster: Cluster, private val coreSiteConfig: Map<String, String>, private val gatewayConf: Map<String, String>)
    : ClusterDetail, LivyCluster, YarnCluster, Cluster by cluster {
    override fun getYarnUIBaseUrl(): String {
        return """$connectionUrl/yarnui/"""
    }

    override fun getLivyConnectionUrl(): String {
        return """$connectionUrl/livy/"""
    }

    override fun getYarnNMConnectionUrl(): String {
        return """${yarnUIBaseUrl}ws/v1/clusters/apps/"""
    }

    override fun getName(): String = this.name()

    override fun getTitle(): String {
        return "$name @${regionName()}"
    }

    override fun getConnectionUrl(): String {
        val httpConnEP = inner().properties().connectivityEndpoints().first { it.name().equals("HTTPS", true) }

        // such as https://hdicluster.azurehdinsight.net
        return """${httpConnEP.name().toLowerCase()}://${httpConnEP.location()}"""
    }

    private enum class GatewayRestAuthCredentialConfigKey(val key: String) {
        ENABLED("restAuthCredential.isEnabled"),
        USERNAME("restAuthCredential.username"),
        PASSWORD("restAuthCredential.password")
    }

    val isGatewayRestAuthCredentialEnabled = gatewayConf[GatewayRestAuthCredentialConfigKey.ENABLED.key]
            ?.equals("true", true) ?: false

    override fun getHttpUserName(): String? {
        return if (isGatewayRestAuthCredentialEnabled) gatewayConf[GatewayRestAuthCredentialConfigKey.USERNAME.key] else null
    }

    override fun getHttpPassword(): String? {
        return if (isGatewayRestAuthCredentialEnabled) gatewayConf[GatewayRestAuthCredentialConfigKey.PASSWORD.key] else null
    }
}