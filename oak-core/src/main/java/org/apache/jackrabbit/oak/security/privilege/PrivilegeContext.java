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
package org.apache.jackrabbit.oak.security.privilege;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.util.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.TreeUtil;

final class PrivilegeContext implements Context, PrivilegeConstants {

    private static final Context INSTANCE = new PrivilegeContext();

    private PrivilegeContext() {
    }

    static Context getInstance() {
        return INSTANCE;
    }

    //------------------------------------------------------------< Context >---
    @Override
    public boolean definesProperty(Tree parent, PropertyState property) {
        return definesTree(parent) && PRIVILEGE_PROPERTY_NAMES.contains(property.getName());
    }

    @Override
    public boolean definesTree(Tree tree) {
        return NT_REP_PRIVILEGE.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    @Override
    public boolean definesLocation(TreeLocation location) {
        return location.getPath().startsWith(PRIVILEGES_PATH);
    }

    @Override
    public boolean hasChildItems(Tree parent) {
        return parent.hasChild(REP_PRIVILEGES)
                || NT_REP_PRIVILEGES.equals(TreeUtil.getPrimaryTypeName(parent))
                || NT_REP_PRIVILEGE.equals(TreeUtil.getPrimaryTypeName(parent));
    }
}
