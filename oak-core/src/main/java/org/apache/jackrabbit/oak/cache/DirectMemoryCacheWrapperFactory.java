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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.Cache;
import com.mongodb.BasicDBObject;
import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.measures.In;
import org.apache.directmemory.measures.Ram;
import org.apache.jackrabbit.oak.plugins.mongomk.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.mongomk.Node;
import org.apache.jackrabbit.oak.plugins.mongomk.Serializers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
                .setDisposalTime(In.minutes(5))
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
        private final KryoPool pool = new KryoPool();

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
            }finally {
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

            kryo.register(BasicDBObject.class,Serializers.BASIC_DB_OBJECT);
            kryo.register(Node.class, Serializers.NODE);
            kryo.register(Node.Children.class, Serializers.CHILDREN);
            kryo.register(MongoDocumentStore.CachedDocument.class, Serializers.DOCUMENTS);

            kryo.setClassLoader(getClass().getClassLoader());
        }

        private void reset(){
            output.clear();
        }
    }

    private static class KryoPool  {

        private final Queue<KryoHolder> objects = new ConcurrentLinkedQueue<KryoHolder>();

        public KryoHolder get(){
            KryoHolder kh;
            if((kh = objects.poll()) == null){
                kh =  new KryoHolder();
            }
            return kh;
        }

        public void done(KryoHolder kh){
            objects.offer(kh);
        }

        public void close() {
            objects.clear();
        }
    }

}
