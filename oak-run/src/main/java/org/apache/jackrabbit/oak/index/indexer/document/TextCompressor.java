/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.index.indexer.document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.util.Base64;

public class TextCompressor {

    public String compress(String text) throws IOException {
        byte[] compressed = compress(text.getBytes(Charsets.UTF_8));
        String compressedText = encode(compressed);
        if (compressed.length > text.length()) {
            return text;
        } else {
            return compressedText;
        }
    }

    public String decompress(String text) throws IOException {
        if (text.startsWith("{")) {
            return text;
        }
        return decompress(decode(text));
    }

    private byte[] decode(String text) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StringReader reader = new StringReader(text);
        Base64.decode(reader, baos);
        return baos.toByteArray();
    }

    private String encode(byte[] compressed) throws IOException {
        try (InputStream is = new ByteArrayInputStream(compressed)) {
            StringWriter writer = new StringWriter();
            Base64.encode(is, writer);
            return writer.toString();
        }
    }

    private byte[] compress(byte[] bytes) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStream out = new DeflaterOutputStream(baos)) {
            out.write(bytes);
            out.close();
        }
        return baos.toByteArray();
    }

    private static String decompress(byte[] bytes) throws IOException {
        InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int len;
        while ((len = in.read(buffer)) > 0)
            baos.write(buffer, 0, len);
        return new String(baos.toByteArray(), "UTF-8");
    }
}
