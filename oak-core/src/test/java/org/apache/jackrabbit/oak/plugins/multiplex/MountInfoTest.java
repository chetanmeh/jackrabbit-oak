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

package org.apache.jackrabbit.oak.plugins.multiplex;

import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.*;

public class MountInfoTest {

    @Test
    public void mountNameInPath() throws Exception{
        MountInfo md = new MountInfo(new Mount("foo"), of("/a", "/b"));
        assertTrue(md.isMounted("/a"));
        assertTrue(md.isMounted("/b"));
        assertTrue(md.isMounted("/b/c/d"));
        assertTrue("dynamic mount path not recognized", md.isMounted("/x/y/oak:foo/a"));
        assertFalse(md.isMounted("/x/y"));
        assertFalse(md.isMounted("/x/y/foo"));
    }
    
    @Test
    public void isWrapped() {
        MountInfo mi = new MountInfo(new Mount("foo"), of("/c", "/f"));
        assertTrue(mi.isWrapped("/a", "/g")); // wraps both
        assertTrue(mi.isWrapped("/b", "/d")); // wraps one
        assertFalse(mi.isWrapped("/a", "/b")); // wraps none ('outside c-f')
        assertFalse(mi.isWrapped("/d", "/e")); // wraps none ('inside c-f')
    }

}