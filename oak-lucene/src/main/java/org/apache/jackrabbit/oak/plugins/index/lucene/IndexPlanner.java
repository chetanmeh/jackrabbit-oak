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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.lucene.index.IndexReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

public class IndexPlanner {
    private static final Logger log = LoggerFactory.getLogger(IndexPlanner.class);
    private final IndexDefinition defn;
    private final String nativeFuncName;
    private final IndexTracker tracker;
    private final Filter filter;
    private final List<OrderEntry> sortOrder;
    private IndexNode indexNode;

    public IndexPlanner(IndexTracker tracker, IndexDefinition defn,
                        Filter filter, List<OrderEntry> sortOrder) {
        this.tracker = tracker;
        this.defn = defn;
        this.nativeFuncName = LuceneIndex.getNativeFunctionName(defn);
        this.filter = filter;
        this.sortOrder = sortOrder;
    }

    List<IndexPlan> getPlans() {
        try {
            IndexPlan.Builder plan = getPlan();

            if(defn.isTestModeEnabled()){
                plan = getTestModePlan(plan);
            }

            return plan != null ? Collections.singletonList(plan.build()) :
                    Collections.<IndexPlan>emptyList();
        } finally {
            close();
        }
    }

    private IndexPlan.Builder getTestModePlan(IndexPlan.Builder plan) {
        //For now we support one full text index which indexes everything
        //That index would be used irrespective of the constraints
        if (plan == null && defn.isFullTextEnabled()){
            plan = defaultPlan();
        }

        //Lucene index yet not ready
        if (plan == null){
            return null;
        }

        return plan.setCostPerExecution(defn.getSimulatedCost())
                   .setCostPerEntry(0);
    }

    private void close(){
        if(indexNode != null){
            indexNode.release();
        }
    }

    private IndexPlan.Builder getPlan() {
        if (hasNativeFunction()) {
            return defaultPlan();
        }

        FullTextExpression ft = filter.getFullTextConstraint();

        if (ft != null) {
            return getFullTextPlans();
        }

        List<String> indexedProps = newArrayListWithCapacity(filter.getPropertyRestrictions().size());
        for(PropertyRestriction pr : filter.getPropertyRestrictions()){
            //Only those properties which are included and not tokenized
            //can be managed by lucene for property restrictions
            if(defn.includeProperty(pr.propertyName)
                    && defn.skipTokenization(pr.propertyName)){
                indexedProps.add(pr.propertyName);
            }
        }

        if(!indexedProps.isEmpty()){
            //TODO Need a way to have better cost estimate to indicate that
            //this index can evaluate more propertyRestrictions natively (if more props are indexed)
            //For now we reduce cost per entry
            IndexPlan.Builder plan = defaultPlan();
            if(plan != null) {
                return plan.setCostPerEntry(1.0 / indexedProps.size());
            }
        }

        //TODO Support for path restrictions
        //TODO support for NodeTypes
        //TODO Support for property existence queries
        //TODO support for nodeName queries
        return null;
    }

    private IndexPlan.Builder defaultPlan() {
        IndexNode in = getIndexNode();
        if(in == null){
            return null;
        }
        return planBuilder().setEstimatedEntryCount(getReader().numDocs());
    }

    private IndexReader getReader() {
        return getIndexNode().getSearcher().getIndexReader();
    }

    /**
     * Checks if there is a native function for current index definition
     */
    private boolean hasNativeFunction() {
        return filter.getPropertyRestriction(nativeFuncName) != null;
    }

    private IndexPlan.Builder getFullTextPlans() {
        if (!defn.isFullTextEnabled()) {
            return null;
        }
        Set<String> relPaths = LuceneIndex.getRelativePaths(filter.getFullTextConstraint());
        if (relPaths.size() > 1) {
            log.warn("More than one relative parent for query " + filter.getQueryStatement());
            // there are multiple "parents", as in
            // "contains(a/x, 'hello') and contains(b/x, 'world')"
            // return new MultiLuceneIndex(filter, root, relPaths).getCost();
            // MultiLuceneIndex not currently implemented
            return null;
        }
        //TODO There might be multiple fulltext enabled indexes then we need to chose a default
        //one
        return defaultPlan();
    }

    private IndexNode getIndexNode(){
        if(indexNode == null){
            indexNode = tracker.acquireIndexNode(defn.getDefinitionPath());
        }
        return indexNode;
    }

    private IndexPlan.Builder planBuilder() {
        return new IndexPlan.Builder()
                .setCostPerExecution(1) // we're local. Low-cost
                .setCostPerEntry(1)
                .setFulltextIndex(defn.isFullTextEnabled())
                .setIncludesNodeData(false) // we should not include node data
                .setFilter(filter)
                .setSortOrder(createSortOrder())
                .setDelayed(true); //Lucene is always async
    }

    @CheckForNull
    private List<OrderEntry> createSortOrder() {
        //TODO Refine later once we make mixed indexes having both
        //full text  and property index
        if(defn.isFullTextEnabled()){
            return Collections.emptyList();
        }

        if(sortOrder == null){
            return null;
        }

        List<OrderEntry> orderEntries = newArrayListWithCapacity(sortOrder.size());
        for(OrderEntry o : sortOrder){
            //sorting can only be done for known/configured properties
            // and whose types are known
            //TODO Can sorting be done for array properties
            if(defn.includeProperty(o.getPropertyName())
                    && o.getPropertyType() != null
                    && !o.getPropertyType().isArray()){
                orderEntries.add(o); //Lucene can manage any order desc/asc
            }
        }
        return orderEntries;
    }

}
