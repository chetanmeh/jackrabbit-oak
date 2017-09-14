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

import java.io.File;
import java.io.StringWriter;
import java.util.Arrays;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public class NodeStateWriterTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private BlobStore blobStore = new MemoryBlobStore();
    private NodeBuilder builder = EMPTY_NODE.builder();
    private StringWriter sw = new StringWriter();

    @Test
    public void newLines() throws Exception{
        NodeStateEntryWriter nw = new NodeStateEntryWriter(blobStore, sw);

        builder.setProperty("foo", 1);
        builder.setProperty("foo2", Arrays.asList("a", "b"), Type.STRINGS);
        builder.setProperty("foo3", "text with \n new line");
        nw.write(new NodeStateEntry(builder.getNodeState(), "/a"));
        nw.close();

        System.out.println(sw);
    }

}