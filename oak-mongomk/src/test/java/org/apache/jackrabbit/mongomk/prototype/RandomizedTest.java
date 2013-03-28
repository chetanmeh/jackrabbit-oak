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
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.json.JsonObject;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.junit.Test;

import com.mongodb.DB;

/**
 * A simple randomized single-instance test.
 */
public class RandomizedTest {
    
    private static final boolean MONGO_DB = false;
    // private static final boolean MONGO_DB = true;
    
    private MongoMK mk;
    private MicroKernelImpl mkGold;
    
    @Test
    public void addRemoveSet() throws Exception {
        mk = createMK();
        mkGold = new MicroKernelImpl();
        Random r = new Random(1);
        int opCount = 1000, nodeCount = 10;
        int propertyCount = 5, valueCount = 10;
        StringBuilder log = new StringBuilder();
        try {
            for (int i = 0; i < opCount; i++) {
                String node = "t" + r.nextInt(nodeCount);
                String property = "p" + r.nextInt(propertyCount);
                String value = "" + r.nextInt(valueCount);
                String diff;
                int op = r.nextInt(3);
                switch(op) {
                case 0:
                    diff = "+ \"" + node + "\": { \"" + property + "\": " + value + "}";
                    log.append(diff).append('\n');
                    commit(diff);
                    break;
                case 1:
                    diff = "- \"" + node + "\"";
                    log.append(diff).append('\n');
                    commit(diff);
                    break;
                case 2:
                    diff = "^ \"" + node + "/" + property + "\": " + value;
                    log.append(diff).append('\n');
                    commit(diff);
                    break;
                case 3:
                    fail();
                }
                get(node);
            }
        } catch (AssertionError e) {
            throw new Exception("log: " + log, e);
        } catch (Exception e) {
            throw new Exception("log: " + log, e);
        }
        mk.dispose();
        mkGold.dispose();
    }
    
    private void get(String node) {
        String headGold = mkGold.getHeadRevision();
        String head = mk.getHeadRevision();
        String p = "/" + node;
        if (!mkGold.nodeExists(p, headGold)) {
            assertFalse(mk.nodeExists(p, head));
            return;
        }
        assertTrue(mk.nodeExists(p, head));
        String resultGold = mkGold.getNodes(p, headGold, 0, 0, Integer.MAX_VALUE, null);
        String result = mk.getNodes(p, head, 0, 0, Integer.MAX_VALUE, null);
        resultGold = normalize(resultGold);
        result = normalize(result);
        assertEquals(resultGold, result);
    }
    
    private static String normalize(String json) {
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');
        JsonObject o = JsonObject.create(t);
        JsopBuilder w = new JsopBuilder();
        o.toJson(w);
        return w.toString();
    }

    private void commit(String diff) {
        boolean ok = false;
        try {
            mkGold.commit("/", diff, null, null);
            ok = true;
        } catch (MicroKernelException e) {
            try {
                mk.commit("/", diff, null, null);
                fail("Should fail: " + diff + " with exception " + e);
            } catch (MicroKernelException e2) {
                // expected
            }
        }
        if (ok) {
            mk.commit("/", diff, null, null);
        }
    }
    
    private static MongoMK createMK() {
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
            return new MongoMK(db, 0);
        }
        return new MongoMK();
    }

}
