package com.microsoft.azure

/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

import com.microsoft.azure.maven.AbstractAzureMojo
import org.apache.maven.plugins.annotations.Mojo

@Mojo(name = "sayHi")
class MyMojo : AbstractAzureMojo() {
    override fun doExecute() {
        log.info("Hello world from kt!")
    }
}
