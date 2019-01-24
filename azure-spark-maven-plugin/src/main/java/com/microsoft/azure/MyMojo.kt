package com.microsoft.azure

/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugin.MojoExecutionException
import org.apache.maven.plugins.annotations.Mojo

@Mojo(name = "sayHi")
class MyMojo : AbstractMojo() {
    @Throws(MojoExecutionException::class)
    override fun execute() {
        log.info("Hello world from kt!")
    }
}
