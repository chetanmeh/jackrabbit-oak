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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.STORE_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexTrackerTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();
    private IndexTracker tracker = new IndexTracker();

    @Test
    public void detectDefinitionChanges() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene",
                ImmutableSet.of(TYPENAME_STRING));

        tracker.update(builder.getNodeState());
        assertEquals(1, tracker.getDefinitions().size());
        assertTrue(tracker.hasDefinition("oak:index/lucene"));

        IndexDefinition dfn = tracker.getDefinition("oak:index/lucene");
        assertTrue(dfn.includePropertyType(Type.STRING.tag()));
        assertFalse(dfn.isStoreNodeName());

        newLuceneIndexDefinition(index, "lucene2",
                ImmutableSet.of(TYPENAME_STRING));
        tracker.update(builder.getNodeState());
        assertEquals(2, tracker.getDefinitions().size());
        assertTrue(tracker.hasDefinition("oak:index/lucene2"));

        builder.child(INDEX_DEFINITIONS_NAME).child("lucene").setProperty(STORE_NODE_NAME, true);
        tracker.update(builder.getNodeState());
        dfn = tracker.getDefinition("oak:index/lucene");
        assertTrue(dfn.isStoreNodeName());
    }
}
