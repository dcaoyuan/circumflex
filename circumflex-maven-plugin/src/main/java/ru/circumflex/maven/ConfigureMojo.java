/*
 * Copyright (C) 2009-2010 Boris Okunskiy (http://incarnate.ru)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package ru.circumflex.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Properties;

/**
 * @goal cfg
 */
public class ConfigureMojo extends AbstractMojo {

    /**
     * @parameter default-value="${project.build.outputDirectory}/cx.properties"
     */
    private File targetFile;

    /**
     * @parameter default-value="true"
     */
    private boolean skipUnresolved;

    /**
     * @parameter expression="${project}"
     * @readonly
     */
    private MavenProject project;

    public void execute() throws MojoExecutionException {
        try {
            PrintWriter fw = new PrintWriter(targetFile);
            getLog().info("Writing Circumflex configuration to " + targetFile);
            try {
                for (Object key : project.getProperties().keySet()) {
                    String value = project.getProperties().get(key).toString().trim();
                    if (!(skipUnresolved && value.startsWith("${") && value.endsWith("}")))
                        fw.println(key + "=" + value);
                    else getLog().warn("Property with key " + key + " is unresolved. To include it, set 'skipUnresolved' to false.");
                }
            } finally {
                fw.close();
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Could not configure Circumflex.", e);
        }
    }

}
