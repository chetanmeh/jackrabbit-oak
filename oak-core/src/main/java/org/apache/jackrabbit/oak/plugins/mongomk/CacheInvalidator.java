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

package org.apache.jackrabbit.oak.plugins.mongomk;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.TreeTraverser;
import com.mongodb.*;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

abstract class CacheInvalidator {
    static final Logger LOG = LoggerFactory.getLogger(CacheInvalidator.class);

    public abstract InvalidationResult invalidateCache();

    public static CacheInvalidator createHierarchicalInvalidator(MongoDocumentStore documentStore) {
        return new HierarchicalInvalidator(documentStore);
    }

    public static CacheInvalidator createLinearInvalidator(MongoDocumentStore documentStore) {
        return new LinearInvalidator(documentStore);
    }

    public static CacheInvalidator createSimpleInvalidator(MongoDocumentStore documentStore) {
        return new SimpleInvalidator(documentStore);
    }

    static class InvalidationResult {
        int invalidationCount;
        int uptodateCount;
        int cacheSize;
        long timeTaken;
        int queryCount;
        int cacheEntriesProcessedCount;

        @Override
        public String toString() {
            return "InvalidationResult{" +
                    "invalidationCount=" + invalidationCount +
                    ", uptodateCount=" + uptodateCount +
                    ", cacheSize=" + cacheSize +
                    ", timeTaken=" + timeTaken +
                    ", queryCount=" + queryCount +
                    ", cacheEntriesProcessedCount=" + cacheEntriesProcessedCount +
                    '}';
        }
    }

    private static class SimpleInvalidator extends CacheInvalidator {
        private final MongoDocumentStore documentStore;

        private SimpleInvalidator(MongoDocumentStore documentStore) {
            this.documentStore = documentStore;
        }

        @Override
        public InvalidationResult invalidateCache() {
            InvalidationResult result = new InvalidationResult();
            Map<String, NodeDocument> cacheMap = documentStore.getCache();
            result.cacheSize = cacheMap.size();
            for (String key : cacheMap.keySet()) {
                documentStore.invalidateCache(Collection.NODES, key);
            }
            return result;
        }
    }

    private static class LinearInvalidator extends CacheInvalidator {
        private final DBCollection nodes;
        private final MongoDocumentStore documentStore;

        public LinearInvalidator(MongoDocumentStore documentStore) {
            this.documentStore = documentStore;
            this.nodes = documentStore.getDBCollection(Collection.NODES);
        }

        @Override
        public InvalidationResult invalidateCache() {
            final Map<String, NodeDocument> cacheMap = documentStore.getCache();
            final InvalidationResult result = new InvalidationResult();
            result.cacheSize = cacheMap.size();

            QueryBuilder query = QueryBuilder.start(Document.ID)
                    .in(cacheMap.keySet());

            //Fetch only the lastRev map and id
            final BasicDBObject keys = new BasicDBObject(NodeDocument.ID, 1);
            keys.put(NodeDocument.MOD_COUNT, 1);

            //Fetch lastRev for each such node
            DBCursor cursor = nodes.find(query.get(), keys);
            result.queryCount++;
            for (DBObject obj : cursor) {
                result.cacheEntriesProcessedCount++;
                String id = (String) obj.get(NodeDocument.ID);
                Number modCount = (Number) obj.get(NodeDocument.MOD_COUNT);

                NodeDocument cachedDoc = documentStore.getIfCached(Collection.NODES, id);
                if (cachedDoc != null
                        && !Objects.equal(cachedDoc.getModCount(), modCount)) {
                    documentStore.invalidateCache(Collection.NODES, id);
                    result.invalidationCount++;
                } else {
                    result.uptodateCount++;
                }
            }
            return result;
        }
    }


    private static class HierarchicalInvalidator extends CacheInvalidator {
        private final DBCollection nodes;
        private final MongoDocumentStore documentStore;

        public HierarchicalInvalidator(MongoDocumentStore documentStore) {
            this.documentStore = documentStore;
            this.nodes = documentStore.getDBCollection(Collection.NODES);
        }

        @Override
        public InvalidationResult invalidateCache() {
            final InvalidationResult result = new InvalidationResult();
            Map<String, NodeDocument> cacheMap = documentStore.getCache();
            TreeNode root = constructTreeFromPaths(cacheMap.keySet());

            //Invalidation stats
            result.cacheSize = cacheMap.size();

            //Time at which the check is started. All NodeDocuments which
            //are found to be uptodate would be marked touched at this time
            final long startTime = System.currentTimeMillis();

            Iterator<TreeNode> treeItr = TRAVERSER.breadthFirstTraversal(root).iterator();
            PeekingIterator<TreeNode> pitr = Iterators.peekingIterator(treeItr);
            Map<String, TreeNode> sameLevelNodes = Maps.newHashMap();

            //Fetch only the lastRev map and id
            final BasicDBObject keys = new BasicDBObject(NodeDocument.ID, 1);
            keys.put(NodeDocument.MOD_COUNT, 1);

            while (pitr.hasNext()) {
                final TreeNode tn = pitr.next();

                //Root node would already have been processed
                //Allows us to save on the extra query for /
                if(tn.isRoot()){
                    tn.markUptodate(startTime);
                    continue;
                }

                //Collect nodes at same level in tree if
                //they are not uptodate.
                if (tn.isUptodate(startTime)) {
                    result.uptodateCount++;
                } else {
                    sameLevelNodes.put(tn.getId(), tn);
                }

                final boolean hasMore = pitr.hasNext();

                //Change in level or last element
                if (!sameLevelNodes.isEmpty() &&
                        ((hasMore && tn.level() != pitr.peek().level()) || !hasMore )) {

                    QueryBuilder query = QueryBuilder.start(Document.ID)
                            .in(sameLevelNodes.keySet());

                    //Fetch lastRev and modCount for each such nodes
                    DBCursor cursor = nodes.find(query.get(), keys);
                    LOG.debug("Checking for changed nodes at level {} with {} paths",tn.level(),sameLevelNodes.size());
                    result.queryCount++;
                    for (DBObject obj : cursor) {

                        result.cacheEntriesProcessedCount++;

                        Number latestModCount = (Number) obj.get(NodeDocument.MOD_COUNT);
                        String id = (String) obj.get(NodeDocument.ID);

                        final TreeNode tn2 = sameLevelNodes.get(id);
                        NodeDocument cachedDoc = tn2.getDocument();
                        if (cachedDoc != null) {
                            boolean noChangeInModCount = Objects.equal(latestModCount, cachedDoc.getModCount());
                            if (noChangeInModCount) {
                                result.uptodateCount++;
                                tn2.markUptodate(startTime);
                            } else {
                                result.invalidationCount++;
                                tn2.invalidate();
                            }
                        }

                        //Remove the processed nodes
                        sameLevelNodes.remove(tn2.getId());
                    }

                    //NodeDocument present in cache but not in database
                    //Remove such nodes from cache
                    if (!sameLevelNodes.isEmpty()) {
                        for (TreeNode leftOverNodes : sameLevelNodes.values()) {
                            leftOverNodes.invalidate();
                        }
                    }

                    sameLevelNodes.clear();
                }
            }

            result.timeTaken = System.currentTimeMillis() - startTime;
            LOG.debug("Cache invalidation details - {}", result);

            //TODO collect the list of ids which are invalidated such that entries for only those
            //ids are removed from the Document Children Cache

            return result;
        }

        private TreeNode constructTreeFromPaths(Set<String> ids) {
            TreeNode root = new TreeNode("");
            for (String id : ids) {
                TreeNode current = root;
                String path = Utils.getPathFromId(id);
                for (String name : PathUtils.elements(path)) {
                    current = current.child(name);
                }
            }
            return root;
        }

        private static TreeTraverser<TreeNode> TRAVERSER = new TreeTraverser<TreeNode>() {
            @Override
            public Iterable<TreeNode> children(TreeNode root) {
                return root.children();
            }
        };


        private class TreeNode {
            private final String name;
            private final TreeNode parent;
            private final String id;

            private final Map<String, TreeNode> children = new HashMap<String, TreeNode>();

            public TreeNode(String name) {
                this(null, name);
            }

            public TreeNode(TreeNode parent, String name) {
                this.name = name;
                this.parent = parent;
                this.id = Utils.getIdFromPath(getPath());
            }

            public TreeNode child(String name) {
                TreeNode child = children.get(name);
                if (child == null) {
                    child = new TreeNode(this, name);
                    children.put(name, child);
                }
                return child;
            }

            public Iterable<TreeNode> children() {
                return children.values();
            }

            public String getId() {
                return id;
            }

            public int level() {
                return Utils.pathDepth(getPath());
            }

            public TreeNode getParent() {
                return parent;
            }

            public boolean isRoot() {
                return name.isEmpty();
            }

            public String getPath() {
                if (isRoot()) {
                    return "/";
                } else {
                    StringBuilder sb = new StringBuilder();
                    buildPath(sb);
                    return sb.toString();
                }
            }

            public void invalidate() {
                LOG.debug("Change detected for {}. Invalidating the cached entry", getId());
                documentStore.invalidateCache(Collection.NODES, getId());
            }

            public NodeDocument getDocument() {
                return documentStore.getIfCached(Collection.NODES, id);
            }

            public boolean isUptodate(long time) {
                NodeDocument doc = documentStore.getIfCached(Collection.NODES, id);
                if (doc != null) {
                    return doc.isUptodate(time);
                } else {
                    //If doc is not present in cache then its already
                    //uptodate i.e. no further consistency check required
                    //for this document
                    return true;
                }
            }

            public void markUptodate(long cacheCheckTime) {
                NodeDocument doc = getDocument();
                if (doc == null) {
                    return;
                }
                markUptodate(cacheCheckTime, doc);
            }

            @Override
            public String toString() {
                return id;
            }

            private void markUptodate(long cacheCheckTime, NodeDocument uptodateRoot) {
                for (TreeNode tn : children.values()) {
                    tn.markUptodate(cacheCheckTime, uptodateRoot);
                }
                ///Update the parent after child
                markUptodate(getId(), cacheCheckTime, uptodateRoot);
            }

            private void markUptodate(String key, long time, NodeDocument uptodateRoot) {
                NodeDocument doc = documentStore.getIfCached(Collection.NODES, key);

                if (doc == null) {
                    return;
                }
                //Only mark the cachedDoc uptodate if
                // 1. it got created i.e. cached document creation
                //    time is greater or same as the time of the root node on which markUptodate
                //    is invoked. As in typical cache population child node would be added
                //    later than the parent.
                //    If the creation time is less then it means that parent got replaced/updated later
                //    and hence its _lastRev property would not truly reflect the state of child nodes
                //    present in cache
                // 2. OR Check if both documents have been marked uptodate in last cycle. As in that case
                //    previous cycle would have done the required checks

                if (doc.getCreated() >= uptodateRoot.getCreated()
                        || doc.getLastCheckTime() == uptodateRoot.getLastCheckTime()) {
                    doc.markUptodate(time);
                }
            }

            private void buildPath(StringBuilder sb) {
                if (!isRoot()) {
                    getParent().buildPath(sb);
                    sb.append('/').append(name);
                }
            }
        }
    }
}