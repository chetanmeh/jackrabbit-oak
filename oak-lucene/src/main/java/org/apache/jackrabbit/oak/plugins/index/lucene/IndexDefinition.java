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
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.jcr.PropertyType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXPERIMENTAL_STORAGE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.FULL_TEXT_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.FUNC_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.STORE_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TEST_MODE_COST;

public class IndexDefinition {
    private static final Logger log = LoggerFactory.getLogger(IndexDefinition.class);
    private final int propertyTypes;

    private final Set<String> excludes;

    private final Set<String> includes;

    private final boolean storageEnabled;

    private final boolean fullTextEnabled;

    private final boolean storeNodeName;

    private final double simulatedCostForTest;

    private final NodeBuilder definition;

    /**
     * Index path and name would only be determined when IndexDefinition is created
     * on the Reader side. On writer side this information is not present as we are
     * only handed over the NodeBuilder
     */

    private final String defnPath;

    private final String name;

    private final String functionName;

    private final Map<String, PropertyDefinition> propDefns;

    public IndexDefinition(NodeState state, String path){
        this(new ReadOnlyBuilder(state), path);
    }

    public IndexDefinition(NodeBuilder definition) {
        this(definition, null);
    }

    public IndexDefinition(NodeBuilder defn, String path) {
        this.definition = defn;
        this.defnPath = path;
        this.name = path != null ? PathUtils.getName(path) : null;
        PropertyState pst = defn.getProperty(INCLUDE_PROPERTY_TYPES);
        if (pst != null) {
            int types = 0;
            for (String inc : pst.getValue(Type.STRINGS)) {
                try {
                    types |= 1 << PropertyType.valueFromName(inc);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown property type: " + inc);
                }
            }
            this.propertyTypes = types;
        } else {
            this.propertyTypes = -1;
        }

        this.excludes = getMultiProperty(defn, EXCLUDE_PROPERTY_NAMES);
        this.includes = getMultiProperty(defn, INCLUDE_PROPERTY_NAMES);
        this.fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);

        //Storage is disabled for non full text indexes
        this.storageEnabled = this.fullTextEnabled && getOptionalValue(defn, EXPERIMENTAL_STORAGE, true);
        this.storeNodeName = getOptionalValue(defn, STORE_NODE_NAME, false);
        this.functionName = getOptionalValue(defn, FUNC_NAME, null);
        this.simulatedCostForTest =defn.hasProperty(TEST_MODE_COST) ?
                defn.getProperty(TEST_MODE_COST).getValue(Type.DOUBLE) : -1;


        Map<String, PropertyDefinition> propDefns = Maps.newHashMap();
        for(String propName : includes){
            if(defn.hasChildNode(propName)){
                propDefns.put(propName, new PropertyDefinition(this, propName, defn.child(propName)));
            }
        }
        this.propDefns = ImmutableMap.copyOf(propDefns);
    }

    int getPropertyTypes() {
        return propertyTypes;
    }

    boolean includeProperty(String name) {
        if(!includes.isEmpty()){
            return includes.contains(name);
        }
        return !excludes.contains(name);
    }

    boolean includePropertyType(int type){
        //TODO Assumption that  propertyTypes = -1 indicates no explicit property
        //type defined hence all types would be included
        if(propertyTypes < 0){
            return true;
        }
        return (propertyTypes & (1 << type)) != 0;

    }

    public boolean skipTokenization(String propertyName){
        if(!isFullTextEnabled()){
            return true;
        }
        return LuceneIndexHelper.skipTokenization(propertyName);
    }

    /**
     * Checks if a given property should be stored in the lucene index or not
     */
    public boolean isStored(String name) {
        return storageEnabled;
    }

    public NodeBuilder getDefinition() {
        return definition;
    }

    public boolean isFullTextEnabled() {
        return fullTextEnabled;
    }

    public boolean isTestModeEnabled() {
        return simulatedCostForTest > 0;
    }

    public double getSimulatedCost() {
        return simulatedCostForTest;
    }

    public boolean isStoreNodeName() {
        return storeNodeName;
    }

    @CheckForNull
    public String getDefinitionPath() {
        return defnPath;
    }

    public String getFunctionName() {
        return functionName != null ? functionName : name;
    }

    @CheckForNull
    public String getName(){
        return name;
    }

    @CheckForNull
    public PropertyDefinition getPropDefn(String propName){
        return propDefns.get(propName);
    }

    public boolean hasPropertyDefinition(String propName){
        return propDefns.containsKey(propName);
    }

    @Override
    public String toString() {
        if(defnPath != null){
            return defnPath;
        }
        return super.toString();
    }

    //~------------------------------------------< Internal >

    private static boolean getOptionalValue(NodeBuilder definition, String propName, boolean defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.BOOLEAN);
    }

    private static String getOptionalValue(NodeBuilder definition, String propName, String defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.STRING);
    }

    private static Set<String> getMultiProperty(NodeBuilder definition, String propName){
        PropertyState pse = definition.getProperty(propName);
        return pse != null ? ImmutableSet.copyOf(pse.getValue(Type.STRINGS)) : Collections.<String>emptySet();
    }
}
