/**
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
package com.microsoft.azure.maven.spark.clusters;


import com.microsoft.azure.maven.spark.errors.HDIException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.List;

public interface ClusterDetail {

    default boolean isEmulator() {
        return false;
    }
    default boolean isConfigInfoAvailable() {
        return false;
    }

    String getName();

    String getTitle();

    @Nullable
    default String getState() {
        return null;
    }

    @Nullable
    default String getLocation() {
        return null;
    }

    String getConnectionUrl();

    @Nullable
    default String getCreateDate() {
        return null;
    }

//    default ClusterType getType() {
//        return null;
//    }

    @Nullable
    default String getVersion() {
        return null;
    }

//    SubscriptionDetail getSubscription();

    default int getDataNodes() {
        return 0;
    }

    @Nullable
    default String getHttpUserName() throws HDIException {
        return null;
    }

    @Nullable
    default String getHttpPassword() throws HDIException {
        return null;
    }

    @Nullable
    default String getOSType() {
        return null;
    }

    @Nullable
    default String getResourceGroup() {
        return null;
    }

//    @Nullable
//    default IHDIStorageAccount getStorageAccount() throws HDIException {
//        return null;
//    }

//    default List<HDStorageAccount> getAdditionalStorageAccounts() {
//        return null;
//    }

    default void getConfigurationInfo() throws IOException, HDIException {
    }

    @Nullable
    default String getSparkVersion() {
        return null;
    }

//    default SparkSubmitStorageType getDefaultStorageType(){
//        return SparkSubmitStorageType.DEFAULT_STORAGE_ACCOUNT;
//    }
//
//    default SparkSubmitStorageTypeOptionsForCluster getStorageOptionsType(){
//        return SparkSubmitStorageTypeOptionsForCluster.ClusterWithFullType;
//    }
}