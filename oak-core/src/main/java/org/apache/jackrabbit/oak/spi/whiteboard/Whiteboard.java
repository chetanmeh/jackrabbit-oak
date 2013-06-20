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
package org.apache.jackrabbit.oak.spi.whiteboard;

import java.util.Map;

public interface Whiteboard {
    /**
     * Default Whiteboard instance providing support for scheduling and
     * JMX registration
     */
    Whiteboard DEFAULT = new DefaultWhiteboard();

    /**
     * Publishes the given service to the whiteboard. Use the returned
     * registration object to unregister the service.
     *
     * @param type type of the service
     * @param service service instance
     * @param properties service properties
     * @return service registration
     */
    <T> Registration register(Class<T> type, T service, Map<?, ?> properties);

}
