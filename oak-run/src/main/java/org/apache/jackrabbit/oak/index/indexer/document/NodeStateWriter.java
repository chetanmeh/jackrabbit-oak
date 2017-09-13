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

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeConstants;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;

public class NodeStateWriter implements Closeable{
    private final Writer writer;
    private final JsopBuilder jw = new JsopBuilder();
    private final JsonSerializer serializer;
    private final Map<String, String> propNameDict = new HashMap<>();
    private boolean propNameDictionaryEnabled;
    private TextCompressor compressor = new TextCompressor();
    private boolean compressionEnabled;

    public NodeStateWriter(BlobStore blobStore, Writer writer) throws IOException {
        this.writer = writer;
        this.serializer = new JsonSerializer(jw, new BlobIdSerializer(blobStore));
    }

    public void write(NodeStateEntry e) throws IOException {
        String test = asText(e.getNodeState());
        writer.append(e.getPath())
                .append("|")
                .append(test)
                .append(LINE_SEPARATOR.value());
    }

    @Override
    public void close() throws IOException {
        writer.flush();
    }

    public Map<String, String> getPropNameDict() {
        return Collections.unmodifiableMap(propNameDict);
    }

    public void setPropNameDictionaryEnabled(boolean propNameDictionaryEnabled) {
        this.propNameDictionaryEnabled = propNameDictionaryEnabled;
    }

    public void setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
    }

    private String asText(NodeState nodeState) throws IOException {
        String json = asJson(nodeState);
        if (compressionEnabled) {
            return compressor.compress(json);
        }
        return json;
    }

    private String asJson(NodeState nodeState) {
        jw.resetWriter();
        jw.object();
        for (PropertyState ps : nodeState.getProperties()) {
            String name = ps.getName();
            if (include(name)) {
                jw.key(processPropertyName(name));
                serializer.serialize(ps);
            }
        }
        jw.endObject();
        return jw.toString();
    }

    private String processPropertyName(String name) {
        if (propNameDictionaryEnabled) {
            String dictName = propNameDict.get(name);
            if (dictName == null) {
                dictName = String.valueOf(propNameDict.size());
                propNameDict.put(name, dictName);
            }
            return dictName;
        }
        return name;
    }

    private boolean include(String name) {
        return !TreeConstants.OAK_CHILD_ORDER.equals(name);
    }
}
