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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.permission.AbstractPermissionRandomTestIT;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

public class MutiplexingProviderRandomTestIT extends AbstractPermissionRandomTestIT {

    private MountInfoProvider mountInfoProvider;

    @Override
    public void before() throws Exception {
        super.before();

        String[] mpxs = new String[] { Iterators.get(allowU.iterator(), allowU.size() / 2) };
        Mounts.Builder builder = Mounts.newBuilder();
        int i = 0;
        for (String p : mpxs) {
            builder.mount("m" + i, p);
            i++;
        }
        mountInfoProvider = builder.build();
    }

    @Override
    protected PermissionProvider candidatePermissionProvider(@Nonnull Root root, @Nonnull String workspaceName,
            @Nonnull Set<Principal> principals) {
        ConfigurationParameters authConfig = ConfigurationParameters.of(Collections.singletonMap(
                AccessControlConstants.PARAM_MOUNT_PROVIDER, Preconditions.checkNotNull(mountInfoProvider)));
        SecurityProviderImpl sp = new SecurityProviderImpl(authConfig);
        AuthorizationConfiguration acConfig = sp.getConfiguration(AuthorizationConfiguration.class);
        return acConfig.getPermissionProvider(root, workspaceName, principals);
    }

}