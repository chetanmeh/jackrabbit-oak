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

import com.google.common.cache.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.directmemory.cache.CacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DirectMemoryCache<V> extends ForwardingCache.SimpleForwardingCache<String,V> implements RemovalListener<String,V>{
    private static final AtomicLong COUNTER = new AtomicLong();

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * As the key names used are path elements the in memory map maintained by DirectMemory
     * consumes lot more memory
     */
    private static final int MAX_ELEMENTS_OFF_HEAP = 500000;
    private final AtomicInteger ai = new AtomicInteger();

    /**
     * Experimental feature in which digest of key is stored instead of actual key
     */
    private final boolean HASH_KEY = true;

    /**
     * It is used to partition key space to use same off heap cache
     * shared between multiple Guava cache
     */
    private final String prefix = "__" + COUNTER.incrementAndGet();
    private final CacheService<String,Object> offHeapCache;

    public DirectMemoryCache(CacheService<String, Object> offHeapCache,Cache<String,V> cache) {
        super(cache);
        this.offHeapCache = offHeapCache;
    }

    @Nullable
    @Override
    public V getIfPresent(Object key) {
        V result = super.getIfPresent(key);
        if(result == null){
            result = retrieve(key);
        }
        return result;
    }


    @Override
    public V get(final String key, final Callable<? extends V> valueLoader) throws ExecutionException {
        return super.get(key, new Callable<V>() {
            @Override
            public V call() throws Exception {
                //Check in offHeap first
                V result = retrieve(key);

                //Not found in L2 then load
                if(result == null){
                    result = valueLoader.call();
                }
                return result;
            }
        });
    }

    @Override
    public ImmutableMap<String, V> getAllPresent(Iterable<?> keys) {
        List<?> list = Lists.newArrayList(keys);
        ImmutableMap<String, V> result = super.getAllPresent(list);

        //All the requested keys found then no
        //need to check L2
        if(result.size() == list.size()){
            return result;
        }

        //Look up value from L2
        Map<String,V> r2 = Maps.newHashMap(result);
        for(Object key : list){
            if(!result.containsKey(key)){
                V val = retrieve(key);
                if(val != null){
                    r2.put((String) key,val);
                }
            }
        }
        return ImmutableMap.copyOf(r2);
    }

    @Override
    public void invalidate(Object key) {
        super.invalidate(key);
        offHeapCache.free(prepareKey(key));
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        super.invalidateAll(keys);
        for(Object key : keys){
            offHeapCache.free(prepareKey(key));
        }
    }

    @Override
    public void invalidateAll() {
        super.invalidateAll();
        //Look for keys which are part of this map and free them
        for(String key : offHeapCache.getMap().keySet()){
            if(currentCacheKey(key)){
                offHeapCache.free(key);
            }
        }
    }

    @Override
    public void onRemoval(RemovalNotification<String, V> notification) {
        if(notification.getCause() == RemovalCause.SIZE){
            if(offHeapCache.entries() > MAX_ELEMENTS_OFF_HEAP){
                if(ai.incrementAndGet() % 100 == 0){
                    logger.warn("Number of entries in off heap cache is exceeding the limit {}. ",MAX_ELEMENTS_OFF_HEAP);
                }
                return;
            }

            String key = notification.getKey();
            String preparedKey = prepareKey(key);
            Object preparedValue =  prepareValue(key,notification.getValue());
            offHeapCache.put(preparedKey,preparedValue);
        }
    }

    private V retrieve(Object key) {
        Object value =  offHeapCache.retrieve(prepareKey(key));
        if(value instanceof KeyAwareValue
                && ((KeyAwareValue) value).actualKey.equals(key)){
            value = ((KeyAwareValue) value).value;
        }
        return (V)value;
    }

    private String prepareKey(Object key){
        if(HASH_KEY && key instanceof String){
            key = digest((String)key);
        }
        return prefix + key;
    }

    private Object prepareValue(String actualKey, V value) {
        if(HASH_KEY){
            return new KeyAwareValue(actualKey,value);
        }
        return value;
    }

    private static String digest(String key) {
        byte[] hash = DigestUtils.sha256(key);
        return Base64.encodeBase64URLSafeString(hash);
    }

    private boolean currentCacheKey(String key) {
        return key.startsWith(prefix);
    }

    private static class KeyAwareValue<V> {
        V value;
        String actualKey;

        public KeyAwareValue() {
        }

        private KeyAwareValue(String actualKey, V value) {
            this.value = value;
            this.actualKey = actualKey;
        }
    }
}
