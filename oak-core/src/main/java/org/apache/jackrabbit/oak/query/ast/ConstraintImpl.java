/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query.ast;

import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * The base class for constraints.
 */
public abstract class ConstraintImpl extends AstElement {
    
    /**
     * Simplify the expression if possible, for example by removing duplicate expressions.
     * For example, "x=1 or x=1" should be simplified to "x=1".
     * 
     * @return the simplified constraint, or "this" if it is not possible to simplify
     */
    public ConstraintImpl simplify() {
        return this;
    }

    /**
     * Evaluate the result using the currently set values.
     *
     * @return true if the constraint matches
     */
    public abstract boolean evaluate();
    
    /**
     * Get the set of property existence conditions that can be derived for this
     * condition. For example, for the condition "x=1 or x=2", the property
     * existence condition is "x is not null". For the condition "x=1 or y=2",
     * there is no such condition. For the condition "x=1 and y=1", there are
     * two (x is not null, and y is not null).
     * 
     * @return the common property existence condition (possibly empty)
     */
    public abstract Set<PropertyExistenceImpl> getPropertyExistenceConditions();
    
    /**
     * Get the (combined) full-text constraint. For constraints of the form
     * "contains(*, 'x') or contains(*, 'y')", the combined expression is
     * returned. If there is none, null is returned. For constraints of the form
     * "contains(*, 'x') or z=1", null is returned as the full-text index cannot
     * be used in this case for filtering (as it might filter out the z=1
     * nodes).
     * 
     * @param s the selector
     * @return the full-text constraint, if there is any, or null if not
     */
    public FullTextExpression getFullTextConstraint(SelectorImpl s) {
        return null;
    }
    
    /**
     * Get the set of selectors for the given condition.
     * 
     * @return the set of selectors (possibly empty)
     */
    public abstract Set<SelectorImpl> getSelectors();
    
    /**
     * Get the map of "in(..)" conditions.
     * 
     * @return the map
     */
    public abstract Map<DynamicOperandImpl, Set<StaticOperandImpl>> getInMap();

    /**
     * Apply the condition to the filter, further restricting the filter if
     * possible. This may also verify the data types are compatible, and that
     * paths are valid.
     *
     * @param f the filter
     */
    public abstract void restrict(FilterImpl f);

    /**
     * Push as much of the condition down to this selector, further restricting
     * the selector condition if possible. This is important for a join: for
     * example, the query
     * "select * from a inner join b on a.x=b.x where a.y=1 and b.y=1", the
     * condition "a.y=1" can be pushed down to "a", and the condition "b.y=1"
     * can be pushed down to "b". That means it is possible to use an index in
     * this case.
     *
     * @param s the selector
     */
    public abstract void restrictPushDown(SelectorImpl s);
    
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (!(other instanceof ConstraintImpl)) {
            return false;
        }
        return toString().equals(other.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

}
