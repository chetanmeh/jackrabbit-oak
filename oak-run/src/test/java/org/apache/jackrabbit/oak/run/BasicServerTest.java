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
package org.apache.jackrabbit.oak.run;

import static org.junit.Assert.assertEquals;

import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.util.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class BasicServerTest {

    @Rule
    public final TemporaryPort temporaryPort = new TemporaryPort();

    private HttpServer server;

    @Before
    public void startServer() throws Exception {
        server = new HttpServer(getPort());
    }

    @After
    public void stopServer() throws Exception {
        server.stop();
    }

    @Test
    public void testServerOk() throws Exception {
        URL server = new URL("http://localhost:" + getPort());
        HttpURLConnection conn = (HttpURLConnection) server.openConnection();
        conn.setRequestProperty("Authorization", "Basic " + Base64.encode("admin:admin"));
        assertEquals(200, conn.getResponseCode());
    }

    private int getPort() {
        return temporaryPort.getPort();
    }

}
