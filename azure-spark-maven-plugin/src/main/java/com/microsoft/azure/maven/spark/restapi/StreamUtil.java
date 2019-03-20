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
package com.microsoft.azure.maven.spark.restapi;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;

import javax.swing.*;
import java.io.*;
import java.net.URL;
import java.util.*;

import static java.util.Objects.requireNonNull;

public class StreamUtil {

    public static String getResultFromInputStream(InputStream inputStream) throws IOException {
//      change string buffer to string builder for thread-safe
        StringBuilder result = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
        }

        return result.toString();
    }

    public static HttpResponse getResultFromHttpResponse(CloseableHttpResponse response) throws IOException {
        int code = response.getStatusLine().getStatusCode();
        String reason = response.getStatusLine().getReasonPhrase();
        // Entity for HEAD is empty
        HttpEntity entity = Optional.ofNullable(response.getEntity())
                .orElse(new StringEntity(""));
        try (InputStream inputStream = entity.getContent()) {
            String response_content = getResultFromInputStream(inputStream);
            return new HttpResponse(code, response_content, new HashMap<String, List<String>>(), reason);
        }
    }

    public static File getResourceFile(String resource) throws IOException {
        File file = null;
        URL res = streamUtil.getClass().getResource(resource);

        if (res == null) {
            throw new FileNotFoundException("The resource " + resource + " isn't found");
        }

        if (res.toString().startsWith("jar:")) {
            InputStream input = null;
            OutputStream out = null;

            try {
                input = requireNonNull(streamUtil.getClass().getResourceAsStream(resource));
                file = File.createTempFile(String.valueOf(new Date().getTime()), ".tmp");
                out = new FileOutputStream(file);

                int read;
                byte[] bytes = new byte[1024];

                while ((read = input.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
            } finally {
                if (input != null) {
                    input.close();
                }

                if (out != null) {
                    out.flush();
                    out.close();
                }

                if (file != null) {
                    file.deleteOnExit();
                }
            }

        } else {
            file = new File(res.getFile());
        }

        return file;
    }

    private static StreamUtil streamUtil = new StreamUtil();
    private static ClassLoader classLoader = requireNonNull(streamUtil.getClass().getClassLoader());
    private static final String SPARK_SUBMISSION_FOLDER = "SparkSubmission";
}
