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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import static java.util.concurrent.Executors.newScheduledThreadPool;

public class SimpleWhiteboard implements Whiteboard {
    private ScheduledExecutorService executor = newScheduledThreadPool(0);
    private MBeanServer mbeanServer;

    public SimpleWhiteboard() {
    }

    public SimpleWhiteboard(ScheduledExecutorService executor, MBeanServer mbeanServer) {
        this.executor = executor;
        this.mbeanServer = mbeanServer;
    }

    @Override
    public <T> Registration register(
            Class<T> type, T service, Map<?, ?> properties) {
        Future<?> future = null;
        if (executor != null && type == Runnable.class) {
            Runnable runnable = (Runnable) service;
            Long period =
                    getValue(properties, "scheduler.period", Long.class);
            if (period != null) {
                Boolean concurrent = getValue(
                        properties, "scheduler.concurrent",
                        Boolean.class, Boolean.FALSE);
                if (concurrent) {
                    future = executor.scheduleAtFixedRate(
                            runnable, period, period, TimeUnit.SECONDS);
                } else {
                    future = executor.scheduleWithFixedDelay(
                            runnable, period, period, TimeUnit.SECONDS);
                }
            }
        }

        ObjectName objectName = null;
        Object name = properties.get("jmx.objectname");
        if (mbeanServer != null && name != null) {
            try {
                if (name instanceof ObjectName) {
                    objectName = (ObjectName) name;
                } else {
                    objectName = new ObjectName(String.valueOf(name));
                }
                mbeanServer.registerMBean(service, objectName);
            } catch (JMException e) {
                // ignore
            }
        }

        final Future<?> f = future;
        final ObjectName on = objectName;
        return new Registration() {
            @Override
            public void unregister() {
                if (f != null) {
                    f.cancel(false);
                }
                if (on != null) {
                    try {
                        mbeanServer.unregisterMBean(on);
                    } catch (JMException e) {
                        // ignore
                    }
                }
            }
        };
    }

    public void close(){
        executor.shutdown();
    }

    @SuppressWarnings("unchecked")
    private static <T> T getValue(
            Map<?, ?> properties, String name, Class<T> type, T def) {
        Object value = properties.get(name);
        if (type.isInstance(value)) {
            return (T) value;
        } else {
            return def;
        }
    }

    private static <T> T getValue(
            Map<?, ?> properties, String name, Class<T> type) {
        return getValue(properties, name, type, null);
    }
}
