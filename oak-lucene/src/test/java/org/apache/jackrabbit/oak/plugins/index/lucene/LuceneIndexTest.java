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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;

import com.google.common.base.Function;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.analysis.Analyzer;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class LuceneIndexTest {

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(
                    new LuceneIndexEditorProvider()));

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    private Set<File> dirs = newHashSet();

    @Test
    public void testLucene() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        List<IndexPlan>  plans = queryIndex.getPlans(filter, null, builder.getNodeState());
        Cursor cursor = queryIndex.query(plans.get(0), indexed);
        assertTrue(cursor.hasNext());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testLuceneLazyCursor() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");

        for(int i = 0; i < LuceneIndex.LUCENE_QUERY_BATCH_SIZE; i++){
            builder.child("parent").child("child"+i).setProperty("foo", "bar");
        }

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        List<String> paths = copyOf(transform(cursor, new Function<IndexRow, String>() {
            public String apply(IndexRow input) {
                return input.getPath();
            }
        }));
        assertTrue(!paths.isEmpty());
        assertEquals(LuceneIndex.LUCENE_QUERY_BATCH_SIZE + 1, paths.size());
    }

    @Test
    public void testLucene2() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar");
        builder.child("a").child("b").child("c").setProperty("foo", "bar");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        // filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        assertTrue(cursor.hasNext());
        assertEquals("/a/b/c", cursor.next().getPath());
        assertEquals("/a/b", cursor.next().getPath());
        assertEquals("/a", cursor.next().getPath());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }


    @Test
    public void testLucene3() throws Exception {
        NodeBuilder index = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        NodeBuilder rules = index.child(INDEX_RULES);
        NodeBuilder fooProp = rules.child("nt:base").child(LuceneIndexConstants.PROP_NODE).child("foo");
        fooProp.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        fooProp.setProperty(LuceneIndexConstants.PROP_INCLUDED_TYPE, PropertyType.TYPENAME_STRING);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar", Type.NAME);
        builder.child("a").child("b").child("c")
                .setProperty("foo", "bar", Type.NAME);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        // filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        assertTrue(cursor.hasNext());
        assertEquals("/a", cursor.next().getPath());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testPathRestrictions() throws Exception {
        NodeBuilder idx = newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "lucene", ImmutableSet.of("foo"), null);
        idx.setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a1").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar");
        builder.child("a").child("b").child("c").setProperty("foo", "bar");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        IndexTracker tracker = new IndexTracker();
        tracker.update(indexed);
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);

        FilterImpl filter = createTestFilter();
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/"));

        filter = createTestFilter();
        filter.restrictPath("/", Filter.PathRestriction.DIRECT_CHILDREN);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/a", "/a1"));

        filter = createTestFilter();
        filter.restrictPath("/a", Filter.PathRestriction.DIRECT_CHILDREN);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/a/b"));

        filter = createTestFilter();
        filter.restrictPath("/a", Filter.PathRestriction.ALL_CHILDREN);
        assertFilter(filter, queryIndex, indexed, ImmutableList.of("/a/b", "/a/b/c"));
    }

    private FilterImpl createTestFilter(){
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        return filter;
    }

    @Test
    public void testTokens() {
        Analyzer analyzer = LuceneIndexConstants.ANALYZER;
        assertEquals(ImmutableList.of("parent", "child"),
                LuceneIndex.tokenize("/parent/child", analyzer));
        assertEquals(ImmutableList.of("p1234", "p5678"),
                LuceneIndex.tokenize("/p1234/p5678", analyzer));
        assertEquals(ImmutableList.of("first", "second"),
                LuceneIndex.tokenize("first_second", analyzer));
        assertEquals(ImmutableList.of("first1", "second2"),
                LuceneIndex.tokenize("first1_second2", analyzer));
        assertEquals(ImmutableList.of("first", "second"),
                LuceneIndex.tokenize("first. second", analyzer));
        assertEquals(ImmutableList.of("first", "second"),
                LuceneIndex.tokenize("first.second", analyzer));

        assertEquals(ImmutableList.of("hello", "world"),
                LuceneIndex.tokenize("hello-world", analyzer));
        assertEquals(ImmutableList.of("hello", "wor*"),
                LuceneIndex.tokenize("hello-wor*", analyzer));
        assertEquals(ImmutableList.of("*llo", "world"),
                LuceneIndex.tokenize("*llo-world", analyzer));
        assertEquals(ImmutableList.of("*llo", "wor*"),
                LuceneIndex.tokenize("*llo-wor*", analyzer));
    }

    @Test
    public void luceneWithFSDirectory() throws Exception{
        //Issue is not reproducible with MemoryNodeBuilder and
        //MemoryNodeState as they cannot determine change in childNode without
        //entering
        NodeStore nodeStore = new SegmentNodeStore();
        final IndexTracker tracker = new IndexTracker();
        ((Observable)nodeStore).addObserver(new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                tracker.update(root);
            }
        });
        builder = nodeStore.getRoot().builder();

        //Also initialize the NodeType registry required for Lucene index to work
        builder.setChildNode(JCR_SYSTEM, INITIAL_CONTENT.getChildNode(JCR_SYSTEM));
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder idxb = newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo", "foo2"), null);
        idxb.setProperty(PERSISTENCE_NAME, PERSISTENCE_FILE);
        idxb.setProperty(PERSISTENCE_PATH, getIndexDir());

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        builder = nodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");

        NodeState indexed = nodeStore.merge(builder, HOOK, CommitInfo.EMPTY);

        assertQuery(tracker, indexed, "foo", "bar");

        builder = nodeStore.getRoot().builder();
        builder.setProperty("foo2", "bar2");
        indexed = nodeStore.merge(builder, HOOK, CommitInfo.EMPTY);

        assertQuery(tracker, indexed, "foo2", "bar2");
    }

    @Test
    public void luceneWithCopyOnReadDir() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo", "foo2"), null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after,CommitInfo.EMPTY);

        File indexRootDir = new File(getIndexDir());
        IndexTracker tracker = new IndexTracker(new IndexCopier(sameThreadExecutor(), indexRootDir));
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo", "bar");

        builder = indexed.builder();
        builder.setProperty("foo2", "bar2");
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo2", "bar2");
    }

    @Test
    public void luceneWithCopyOnReadDirAndReindex() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", ImmutableSet.of("foo", "foo2", "foo3"), null);

        //1. Create index in two increments
        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");

        NodeState indexed = HOOK.processCommit(before, builder.getNodeState(),CommitInfo.EMPTY);

        IndexCopier copier = new IndexCopier(sameThreadExecutor(), new File(getIndexDir()));
        IndexTracker tracker = new IndexTracker(copier);
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo", "bar");
        assertEquals(0, copier.getInvalidFileCount());

        builder = indexed.builder();
        builder.setProperty("foo2", "bar2");
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo2", "bar2");
        assertEquals(0, copier.getInvalidFileCount());

        //2. Reindex. This would create index with different index content
        builder = indexed.builder();
        builder.child(INDEX_DEFINITIONS_NAME).child("lucene").setProperty(REINDEX_PROPERTY_NAME, true);
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);

        assertQuery(tracker, indexed, "foo2", "bar2");
        //If reindex case handled properly then invalid count should be zero
        assertEquals(0, copier.getInvalidFileCount());
        assertEquals(2, copier.getIndexDir("/oak:index/lucene").listFiles().length);

        //3. Update again. Now with close of previous reader
        //orphaned directory must be removed
        builder = indexed.builder();
        builder.setProperty("foo3", "bar3");
        indexed = HOOK.processCommit(indexed, builder.getNodeState(),CommitInfo.EMPTY);
        tracker.update(indexed);
        assertQuery(tracker, indexed, "foo3", "bar3");
        assertEquals(0, copier.getInvalidFileCount());
        assertEquals(1, copier.getIndexDir("/oak:index/lucene").listFiles().length);
    }

    @After
    public void cleanUp(){
        for (File d: dirs){
            FileUtils.deleteQuietly(d);
        }
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeState system = root.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private void assertQuery(IndexTracker tracker, NodeState indexed, String key, String value){
        AdvancedQueryIndex queryIndex = new LucenePropertyIndex(tracker);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty(key, Operator.EQUAL,
                PropertyValues.newString(value));
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);
        assertTrue(cursor.hasNext());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    private static List<String> assertFilter(Filter filter, AdvancedQueryIndex queryIndex,
                                             NodeState indexed, List<String> expected) {
        List<IndexPlan> plans = queryIndex.getPlans(filter, null, indexed);
        Cursor cursor = queryIndex.query(plans.get(0), indexed);

        List<String> paths = newArrayList();
        while (cursor.hasNext()) {
            paths.add(cursor.next().getPath());
        }
        Collections.sort(paths);
        for (String p : expected) {
            assertTrue("Expected path " + p + " not found", paths.contains(p));
        }
        assertEquals("Result set size is different", expected.size(), paths.size());
        return paths;
    }

    private String getIndexDir(){
        File dir = new File("target", "indexdir"+System.nanoTime());
        dirs.add(dir);
        return dir.getAbsolutePath();
    }
}
