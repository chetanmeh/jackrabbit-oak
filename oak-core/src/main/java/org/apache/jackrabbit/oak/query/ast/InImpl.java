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
package org.apache.jackrabbit.oak.query.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;

/**
 * A "in" comparison operation.
 */
public class InImpl extends ConstraintImpl {

    private final DynamicOperandImpl operand1;
    private final List<StaticOperandImpl> operand2List;

    public InImpl(DynamicOperandImpl operand1, List<StaticOperandImpl> operand2List) {
        this.operand1 = operand1;
        this.operand2List = operand2List;
    }

    public DynamicOperandImpl getOperand1() {
        return operand1;
    }

    public List<StaticOperandImpl> getOperand2List() {
        return operand2List;
    }
    
    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        PropertyExistenceImpl p = operand1.getPropertyExistence();
        if (p == null) {
            return Collections.emptySet();
        }
        return Collections.singleton(p);
    }

    @Override
    public boolean evaluate() {
        // JCR 2.0 spec, 6.7.16 Comparison:
        // "operand1 may evaluate to an array of values"
        PropertyValue p1 = operand1.currentProperty();
        if (p1 == null) {
            return false;
        }
        for (StaticOperandImpl operand2 : operand2List) {
            PropertyValue p2 = operand2.currentValue();
            if (p2 == null) {
                // if the property doesn't exist, the result is false
                continue;
            }
            int v1Type = ComparisonImpl.getType(p1, p2.getType().tag());
            if (v1Type != p2.getType().tag()) {
                // "the value of operand2 is converted to the
                // property type of the value of operand1"
                p2 = PropertyValues.convert(p2, v1Type, query.getNamePathMapper());
            }
            if (PropertyValues.match(p1, p2)) {
                return true;
            }
        }
        return false;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(operand1).append(" in(");
        int i = 0;
        for (StaticOperandImpl operand2 : operand2List) {
            if (i++ > 0) {
                buff.append(", ");
            }
            buff.append(operand2);
        }
        buff.append(")");
        return buff.toString();
    }

    @Override
    public void restrict(FilterImpl f) {
        ArrayList<PropertyValue> list = new ArrayList<PropertyValue>();
        for (StaticOperandImpl s : operand2List) {
            if (!PropertyValues.canConvert(
                    s.getPropertyType(), 
                    operand1.getPropertyType())) {
                throw new IllegalArgumentException(
                        "Unsupported conversion from property type " + 
                                PropertyType.nameFromValue(s.getPropertyType()) + 
                                " to property type " +
                                PropertyType.nameFromValue(operand1.getPropertyType()));
            }
            list.add(s.currentValue());
        }
        if (list != null) {
            operand1.restrictList(f, list);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        for (StaticOperandImpl op : operand2List) {
            if (op.currentValue() == null) {
                // one unknown value means it is not pushed down
                return;
            }
        }
        if (operand1.canRestrictSelector(s)) {
            s.restrictSelector(this);
        }
    }
    
}
