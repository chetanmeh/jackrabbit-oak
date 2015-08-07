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

package org.apache.jackrabbit.oak.run.osgi;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.RepositoryFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.felix.connect.launch.BundleDescriptor;
import org.apache.felix.connect.launch.ClasspathScanner;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.felix.connect.launch.PojoServiceRegistryFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * RepositoryFactory which constructs an instance of Oak repository. Thi factory supports following
 * parameters
 *
 *  <dl>
 *      <dt>org.osgi.framework.BundleActivator</dt>
 *      <dd>(Optional) BundleActivator instance which would be notified about the startup and shutdown</dd>
 *
 *      <dt>org.apache.jackrabbit.oak.repository.config</dt>
 *      <dd>(Optional) Config key which refers to the map of config where key in that map refers to OSGi config</dd>
 *
 *      <dt>org.apache.jackrabbit.oak.repository.configFile</dt>
 *      <dd>
 *          Comma separated list of file names which referred to config stored in form of JSON. The
 *          JSON content consist of pid as the key and config map as the value
 *      </dd>
 *
 *      <dt>org.apache.jackrabbit.repository.home</dt>
 *      <dd>Used to specify the absolute path of the repository home directory</dd>
 *  </dl>
 */
public class OakOSGiRepositoryFactory implements RepositoryFactory {

    private static Logger log = LoggerFactory.getLogger(OakOSGiRepositoryFactory.class);

    /**
     * Name of the repository home parameter.
     */
    public static final String REPOSITORY_HOME
            = "org.apache.jackrabbit.repository.home";

    /**
     * Timeout in seconds for the repository startup should wait
     */
    public static final String REPOSITORY_STARTUP_TIMEOUT
            = "org.apache.jackrabbit.oak.repository.startupTimeOut";

    /**
     * Config key which refers to the map of config where key in that map refers to OSGi
     * config
     */
    public static final String REPOSITORY_CONFIG = "org.apache.jackrabbit.oak.repository.config";

    /**
     * Comma separated list of file names which referred to config stored in form of JSON. The
     * JSON content consist of pid as the key and config map as the value
     */
    public static final String REPOSITORY_CONFIG_FILE = "org.apache.jackrabbit.oak.repository.configFile";

    /**
     * Default timeout for repository creation
     */
    private static final int DEFAULT_TIMEOUT = (int) TimeUnit.MINUTES.toSeconds(10);

    private static final BundleActivator NOOP = new BundleActivator() {
        @Override
        public void start(BundleContext bundleContext) throws Exception {

        }
        @Override
        public void stop(BundleContext bundleContext) throws Exception {

        }
    };

    @SuppressWarnings("unchecked")
    public Repository getRepository(Map parameters) throws RepositoryException {
        if(parameters == null || !parameters.containsKey(REPOSITORY_HOME)){
            //Required param missing so repository cannot be created
            return null;
        }

        Map config = new HashMap();
        config.putAll(parameters);

        PojoServiceRegistry registry = initializeServiceRegistry(config);
        BundleActivator activator = getApplicationActivator(config);

        try {
            activator.start(registry.getBundleContext());
        } catch (Exception e) {
            log.warn("Error occurred while starting activator {}", activator.getClass(), e);
        }

        //Future which would be used to notify when repository is ready
        // to be used
        SettableFuture<Repository> repoFuture = SettableFuture.create();

        new RunnableJobTracker(registry.getBundleContext());

        //Start the tracker for repository creation
        new RepositoryTracker(registry, activator, repoFuture);


        //Now wait for repository to be created with given timeout
        //if repository creation takes more time. This is required to handle case
        // where OSGi runtime fails to start due to bugs (like cycles)
        int timeout = getTimeoutInSeconds(config);
        try {
            return repoFuture.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RepositoryException("Repository initialization was interrupted");
        } catch (ExecutionException e) {
            throw new RepositoryException(e);
        } catch (TimeoutException e) {
            try {
                shutdown(registry);
            } catch (BundleException be) {
                log.warn("Error occurred while shutting down the service registry (due to " +
                        "startup timeout) backing the Repository ", be);
            }
            throw new RepositoryException("Repository could not be started in " +
                    timeout + " seconds", e);
        }
    }

    @SuppressWarnings("unchecked")
    PojoServiceRegistry initializeServiceRegistry(Map config) {
        processConfig(config);

        PojoServiceRegistry registry = createServiceRegistry(config);
        startConfigTracker(registry, config);
        preProcessRegistry(registry);
        startBundles(registry);
        postProcessRegistry(registry);

        return registry;
    }

    /**
     * Enables pre processing of service registry by sub classes. This can be
     * used to register services before any bundle gets started
     *
     * @param registry service registry
     */
    protected void preProcessRegistry(PojoServiceRegistry registry) {

    }

    /**
     * Enables post processing of service registry e.g. registering new services etc
     * by sub classes
     *
     * @param registry service registry
     */
    protected void postProcessRegistry(PojoServiceRegistry registry) {

    }

    protected List<BundleDescriptor> processDescriptors(List<BundleDescriptor> descriptors) {
        Collections.sort(descriptors, new BundleDescriptorComparator());
        return descriptors;
    }

    static void shutdown(PojoServiceRegistry registry) throws BundleException {
        if (registry != null) {
            registry.getBundleContext().getBundle().stop();
        }
    }

    private static void startConfigTracker(PojoServiceRegistry registry, Map config) {
        new ConfigTracker(config, registry.getBundleContext());
    }

    private static int getTimeoutInSeconds(Map config) {
        Integer timeout = (Integer) config.get(REPOSITORY_STARTUP_TIMEOUT);
        if (timeout == null) {
            timeout = DEFAULT_TIMEOUT;
        }
        return timeout;
    }

    /**
     * Return the BundleActivator provided by the embedding application
     * @param config config passed to factory for initialization
     * @return BundleActivator instance
     */
    private static BundleActivator getApplicationActivator(Map config) {
        BundleActivator activator = (BundleActivator) config.get(BundleActivator.class.getName());
        if (activator == null){
            activator = NOOP;
        }
        return activator;
    }

    @SuppressWarnings("unchecked")
    private static void processConfig(Map config) {
        String home = (String) config.get(REPOSITORY_HOME);
        checkNotNull(home, "Repository home not defined via [%s]", REPOSITORY_HOME);

        home = FilenameUtils.normalizeNoEndSeparator(home);

        String bundleDir = FilenameUtils.concat(home, "bundles");
        config.put(Constants.FRAMEWORK_STORAGE, bundleDir);

        //FIXME Pojo SR currently reads this from system property instead of Framework Property
        config.put(Constants.FRAMEWORK_STORAGE, bundleDir);

        //Directory used by Felix File Install to watch for configs
        config.put("felix.fileinstall.dir", FilenameUtils.concat(home, "config"));

        //Set log level for config to INFO LogService.LOG_INFO
        config.put("felix.fileinstall.log.level", "3");

        //This ensures that configuration is registered in main thread
        //and not in a different thread
        config.put("felix.fileinstall.noInitialDelay", "true");

        config.put("repository.home", FilenameUtils.concat(home, "repository"));

    }

    private PojoServiceRegistry createServiceRegistry(Map<String, Object> config) {
        try {
            ServiceLoader<PojoServiceRegistryFactory> loader = ServiceLoader.load(PojoServiceRegistryFactory.class);
            return loader.iterator().next().newPojoServiceRegistry(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void startBundles(PojoServiceRegistry registry) {
        try {
            List<BundleDescriptor> descriptors = new ClasspathScanner().scanForBundles();
            descriptors = Lists.newArrayList(descriptors);
            descriptors = processDescriptors(descriptors);
            registry.startBundles(descriptors);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class RepositoryTracker extends ServiceTracker<Repository, Repository> {
        private final SettableFuture<Repository> repoFuture;
        private final PojoServiceRegistry registry;
        private final BundleActivator activator;
        private RepositoryProxy proxy;

        public RepositoryTracker(PojoServiceRegistry registry, BundleActivator activator, SettableFuture<Repository> repoFuture) {
            super(registry.getBundleContext(), Repository.class.getName(), null);
            this.repoFuture = repoFuture;
            this.registry = registry;
            this.activator = activator;
            this.open();
        }

        @Override
        public Repository addingService(ServiceReference<Repository> reference) {
            Repository service = context.getService(reference);
            if (proxy == null) {
                //As its possible that future is accessed before the service
                //get registered with tracker. We also capture the initial reference
                //and use that for the first access case
                repoFuture.set(createProxy(service));
            }
            return service;
        }

        @Override
        public void removedService(ServiceReference reference, Repository service) {
            if (proxy != null) {
                proxy.clearInitialReference();
            }
        }

        public PojoServiceRegistry getRegistry() {
            return registry;
        }

        private Repository createProxy(Repository service) {
            proxy = new RepositoryProxy(this, service);
            return (Repository) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{Repository.class, JackrabbitRepository.class, ServiceRegistryProvider.class}, proxy);
        }

        public void shutdownRepository() throws BundleException {
            try {
                activator.stop(getRegistry().getBundleContext());
            } catch (Exception e) {
                log.warn("Error occurred while shutting down activator {}", activator.getClass(), e);
            }
            shutdown(getRegistry());
        }
    }

    /**
     * Due to the way SecurityConfiguration is managed in OSGi env its possible
     * that repository gets created/shutdown few times. So need to have a proxy
     * to access the latest service
     */
    private static class RepositoryProxy implements InvocationHandler {
        private final RepositoryTracker tracker;
        private Repository initialService;

        private RepositoryProxy(RepositoryTracker tracker, Repository initialService) {
            this.tracker = tracker;
            this.initialService = initialService;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object obj = tracker.getService();
            if (obj == null) {
                obj = initialService;
            }

            checkNotNull(obj, "Repository service is not available");

            final String name = method.getName();
            if ("shutdown".equals(name)) {
                tracker.shutdownRepository();
            } else if ("getServiceRegistry".equals(name)){
                return tracker.getRegistry();
            }

            return method.invoke(obj, args);
        }

        public void clearInitialReference() {
            this.initialService = null;
        }
    }
}
