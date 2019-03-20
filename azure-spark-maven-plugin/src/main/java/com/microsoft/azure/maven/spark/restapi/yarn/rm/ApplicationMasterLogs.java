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
package com.microsoft.azure.maven.spark.restapi.yarn.rm;


import org.checkerframework.checker.nullness.qual.Nullable;

public class ApplicationMasterLogs {
    @Nullable
    private String stderr;
    @Nullable
    private String stdout;
    @Nullable
    private String directoryInfo;

    public ApplicationMasterLogs(@Nullable String standout, @Nullable String standerr, @Nullable String directoryInfo) {
        this.stdout = standout;
        this.stderr = standerr;
        this.directoryInfo = directoryInfo;
    }

    public ApplicationMasterLogs() {

    }

    @Nullable
    public String getStderr() {
        return stderr;
    }

    public void setStderr(@Nullable String stderr) {
        this.stderr = stderr;
    }

    @Nullable
    public String getStdout() {
        return stdout;
    }

    public void setStdout(@Nullable String stdout) {
        this.stdout = stdout;
    }

    @Nullable
    public String getDirectoryInfo() {
        return directoryInfo;
    }

    public void setDirectoryInfo(@Nullable String directoryInfo) {
        this.directoryInfo = directoryInfo;
    }
}
