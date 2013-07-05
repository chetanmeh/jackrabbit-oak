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

package org.apache.jackrabbit.oak.cache;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.Cache;
import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.measures.In;
import org.apache.directmemory.measures.Ram;
import org.apache.jackrabbit.oak.plugins.mongomk.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.mongomk.Node;
import org.apache.jackrabbit.oak.plugins.mongomk.Revision;
import org.apache.jackrabbit.oak.plugins.mongomk.Serializers;
import org.apache.jackrabbit.oak.util.SimplePool;

import java.io.Closeable;
import java.io.IOException;

public class DirectMemoryCacheWrapperFactory<V> implements CacheWrapperFactory<V>, Closeable {
    private final int BUFFER_SIZE = Ram.Gb(1);
    private final CacheService<String, Object> cacheService;
    private final KryoSerializer serializer = new KryoSerializer();

    public DirectMemoryCacheWrapperFactory(long size) {
        int noOfBuffers = Math.max(1,(int) (size / BUFFER_SIZE));
        int buffSize = (int) Math.min(size,BUFFER_SIZE);
        cacheService = new DirectMemory<String, Object>()
                .setNumberOfBuffers(noOfBuffers)
                .setSize(buffSize)
                .setSerializer(serializer)
                .setDisposalTime(In.minutes(15))
                .newCacheService();
    }

    @Override
    public Cache<String, V> wrap(Cache<String, V> cache, ForwardingListener<String, V> listener) {
        DirectMemoryCache<V> dmc = new DirectMemoryCache<V>(cacheService, cache);
        listener.setDelegate(dmc);
        return dmc;
    }

    @Override
    public void close() throws IOException {
        //The 0.1-incubator class is not implementing Closeable but
        //0.2-SNAPSHOT does
        if (cacheService instanceof Closeable) {
            ((Closeable) cacheService).close();
        }

        serializer.close();
    }

    private static final class KryoSerializer
            implements org.apache.directmemory.serialization.Serializer {
        //Kryo class is not thread safe so using a pool
        private KryoPool pool = new KryoPool("kryo", 30);

        /**
         * {@inheritDoc}
         */
        @Override
        public <T> byte[] serialize(T obj)
                throws IOException {
            Class<?> clazz = obj.getClass();

            KryoHolder kh = null;
            try {
                kh = pool.get();
                kh.reset();
                checkRegiterNeeded(kh.kryo, clazz);

                kh.kryo.writeObject(kh.output, obj);
                return kh.output.toBytes();
            } catch (InterruptedException e) {
                //TODO Handle it properly
                throw new RuntimeException(e);
            } finally {
                if (kh != null) {
                    pool.done(kh);
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public <T> T deserialize(byte[] source, Class<T> clazz)
                throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
            KryoHolder kh = null;
            try {
                kh = pool.get();
                checkRegiterNeeded(kh.kryo, clazz);

                Input input = new Input(source);
                return kh.kryo.readObject(input, clazz);
            } catch (InterruptedException e) {
                //TODO Handle it properly
                throw new RuntimeException(e);
            } finally {
                if (kh != null) {
                    pool.done(kh);
                }
            }
        }

        public void close() {
            pool.close();
        }

        private void checkRegiterNeeded(Kryo kryo, Class<?> clazz) {
            kryo.register(clazz);
        }

    }

    private static class KryoHolder {
        private static final int BUFFER_SIZE = 1024;
        final Kryo kryo;
        final Output output = new Output(BUFFER_SIZE, -1);

        private KryoHolder() {
            kryo = new Kryo();
            kryo.setReferences(false);
            kryo.setAutoReset(true);
            //kryo.setRegistrationRequired(true);

            kryo.register(Node.class, Serializers.NODE);
            kryo.register(Node.Children.class, Serializers.CHILDREN);
            kryo.register(MongoDocumentStore.CachedDocument.class, Serializers.DOCUMENTS);
        }

        private void reset(){
            output.clear();
        }
    }

    private static class KryoPool extends SimplePool<KryoHolder> {

        public KryoPool(String name, int size) {
            super(name, size);
        }

        @Override
        protected KryoHolder createNew() {
            return new KryoHolder();
        }

        @Override
        public void close() {
            super.close();
        }
    }

}
