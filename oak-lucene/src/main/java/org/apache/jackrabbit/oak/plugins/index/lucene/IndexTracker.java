/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

class IndexTracker {

    /** Logger instance. */
    private static final Logger log =
            LoggerFactory.getLogger(IndexTracker.class);

    private NodeState root = EMPTY_NODE;

    private volatile Map<String, IndexNode> indices = emptyMap();

    private volatile Map<String, IndexDefinition> definitions = emptyMap();

    synchronized void close() {
        Map<String, IndexNode> indices = this.indices;
        this.indices = emptyMap();

        for (Map.Entry<String, IndexNode> entry : indices.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.error("Failed to close the Lucene index at " + entry.getKey(), e);
            }
        }
    }

    synchronized void update(NodeState root) {
        Map<String, IndexNode> original = indices;
        Map<String, IndexDefinition> originalDefns = definitions;
        final Map<String, IndexNode> updates = newHashMap();
        final Map<String, IndexDefinition> defnUpdates = newHashMap();

        List<Editor> editors = newArrayListWithCapacity(original.size() + 1);

        editors.add(new SubtreeEditor(new IndexDefnEditor(defnUpdates, INDEX_DEFINITIONS_NAME)
                , INDEX_DEFINITIONS_NAME));
        for (Map.Entry<String, IndexNode> entry : original.entrySet()) {
            final String path = entry.getKey();
            final String name = entry.getValue().getName();

            editors.add(new SubtreeEditor(new DefaultEditor() {
                @Override
                public void leave(NodeState before, NodeState after) {
                    try {
                        // TODO: Use DirectoryReader.openIfChanged()
                        IndexNode index = IndexNode.open(name, after);
                        updates.put(path, index); // index can be null
                    } catch (IOException e) {
                        log.error("Failed to open Lucene index at " + path, e);
                    }
                }
            }, Iterables.toArray(PathUtils.elements(path), String.class)));
        }

        EditorDiff.process(CompositeEditor.compose(editors), this.root, root);
        this.root = root;

        if (!updates.isEmpty()) {
            indices = ImmutableMap.<String, IndexNode>builder()
                    .putAll(filterKeys(original, not(in(updates.keySet()))))
                    .putAll(filterValues(updates, notNull()))
                    .build();

            for (String path : updates.keySet()) {
                IndexNode index = original.get(path);
                try {
                    index.close();
                } catch (IOException e) {
                    log.error("Failed to close Lucene index at " + path, e);
                }
            }
        }

        if(!defnUpdates.isEmpty()){
            definitions = ImmutableMap.<String, IndexDefinition>builder()
                    .putAll(filterKeys(originalDefns, not(in(defnUpdates.keySet()))))
                    .putAll(filterValues(defnUpdates, notNull()))
                    .build();
        }
    }

    IndexNode acquireIndexNode(String path) {
        IndexNode index = indices.get(path);
        if (index != null && index.acquire()) {
            return index;
        } else {
            return findIndexNode(path);
        }
    }

    Collection<IndexDefinition> getDefinitions() {
        return definitions.values();
    }

    IndexDefinition getDefinition(String path){
        return definitions.get(path);
    }

    boolean hasDefinition(String path){
        return definitions.containsKey(path);
    }

    int getDefinitionCount(){
        return definitions.size();
    }

    Set<String> getIndexNodePaths(){
        return indices.keySet();
    }

    private synchronized IndexNode findIndexNode(String path) {
        // Retry the lookup from acquireIndexNode now that we're
        // synchronized. The acquire() call is guaranteed to succeed
        // since the close() method is also synchronized.
        IndexNode index = indices.get(path);
        if (index != null) {
            checkState(index.acquire());
            return index;
        }

        NodeState node = root;
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }

        final String indexName = PathUtils.getName(path);
        try {
            if (isLuceneIndexNode(node)) {
                index = IndexNode.open(indexName, node);
                if (index != null) {
                    checkState(index.acquire());
                    indices = ImmutableMap.<String, IndexNode>builder()
                            .putAll(indices)
                            .put(path, index)
                            .build();
                    return index;
                }
            } else if (node.exists()) {
                log.warn("Cannot open Lucene Index at path {} as the index is not of type {}", path, TYPE_LUCENE);
            }
        } catch (IOException e) {
            log.error("Could not access the Lucene index at " + path, e);
        }

        return null;
    }

    private static boolean isLuceneIndexNode(NodeState node){
        return TYPE_LUCENE.equals(node.getString(TYPE_PROPERTY_NAME));
    }

    /**
     * Editor to diff the child nodes of /oak:index so as to update the IndexDefinitions
     */
    private static class IndexDefnEditor extends DefaultEditor {
        final Map<String, IndexDefinition> defnUpdates;
        final String basePath;

        IndexDefnEditor(Map<String, IndexDefinition> defnUpdates, String basePath) {
            this.defnUpdates = defnUpdates;
            this.basePath = basePath;
        }

        @Override @CheckForNull
        public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
            if(isLuceneIndexNode(after)) {
                addEntry(name, after);
            }
            return null;
        }

        @Override @CheckForNull
        public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            if(isLuceneIndexNode(after)) {
                addEntry(name, after);
            }
            return null;
        }

        @Override @CheckForNull
        public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            if(isLuceneIndexNode(before)) {
                defnUpdates.put(createPath(name), null);
            }
            return null;
        }

        private void addEntry(String name, NodeState state){
            String path = createPath(name);
            defnUpdates.put(path, new IndexDefinition(state, path));
        }

        private String createPath(String childName){
            return PathUtils.concat(basePath, childName);
        }
    }

}
