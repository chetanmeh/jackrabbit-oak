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

package org.apache.jackrabbit.oak.plugins.index.aggregate;

import java.util.List;

import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class AggregateAdvanceQueryIndex extends AggregateIndex implements QueryIndex.AdvancedQueryIndex {
    private final AdvancedQueryIndex advIndex;

    public AggregateAdvanceQueryIndex(FulltextQueryIndex index){
        super(index);
        this.advIndex = (AdvancedQueryIndex) index;
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        return advIndex.getPlans(filter, sortOrder, rootState);
    }

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        if(!plan.isFulltextIndex()){
            return advIndex.getPlanDescription(plan, root);
        }
        return super.getPlan(plan.getFilter(), root);
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState rootState) {
        if (!plan.isFulltextIndex()) {
            return advIndex.query(plan, rootState);
        }
        return super.query(plan.getFilter(), rootState);
    }
}
