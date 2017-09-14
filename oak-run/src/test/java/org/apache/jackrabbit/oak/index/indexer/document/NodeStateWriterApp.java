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

package org.apache.jackrabbit.oak.index.indexer.document;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Set;
import java.util.function.Consumer;

import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Files;
import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;

public class NodeStateWriterApp {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private IndexingProgressReporter progressReporter =
            new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);

    private String url = "mongodb://localhost:27017/aem-author-62";

    private StatisticsProvider statsProvider;
    private MongoConnection mongoConnection;
    private DocumentNodeStore dns;
    private MongoDocumentStore mds;
    private NodeStoreFixture fixture;

    @Before
    public void setUp() throws Exception {
        String[] args = {url};
        Options opts = prepareOpts(args);

        fixture = NodeStoreFixtureProvider.create(opts);
        dns = (DocumentNodeStore) fixture.getStore();

        Whiteboard wb = opts.getWhiteboard();
        mds = WhiteboardUtils.getService(wb, MongoDocumentStore.class);
        statsProvider = WhiteboardUtils.getService(wb, StatisticsProvider.class);
        mongoConnection = WhiteboardUtils.getService(wb, MongoConnection.class);
        configureEstimators();
    }


    @Test
    public void dumpLocalRepo() throws Exception{
        NodeStateEntryProvider nsep = new NodeStateEntryProvider(dns, mds);
        nsep.withPathFilter(path -> !path.contains("/:"));
        nsep.withProgressReporter(newProgressReporter());

        File dir = new File("/home/chetanm/data/oak/oak-run-indexing/document-traversal");
        File file = new File(dir, "nodes.txt");
        try(Writer writer = Files.newWriter(file, UTF_8)) {
            NodeStateEntryWriter nsw = new NodeStateEntryWriter(dns.getBlobStore(), writer);
            nsw.setPropNameDictionaryEnabled(true);
            nsw.setCompressionEnabled(true);
            for (NodeStateEntry e : nsep) {
                nsw.write(e);
            }

            writeDictionary(dir, nsw);
        }
    }

    private void writeDictionary(File dir, NodeStateEntryWriter nsw) throws IOException {
        JSONObject jo = new JSONObject(nsw.getPropNameDict());
        File dict = new File(dir, "dict.json");
        Files.write(JsopBuilder.prettyPrint(jo.toJSONString()), dict, UTF_8);
    }

    @NotNull
    private Consumer<String> newProgressReporter() {
        return id -> {
            try {
                progressReporter.traversedNode(() -> id);
            } catch (CommitFailedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Options prepareOpts(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        Options opts = new Options().withDisableSystemExit();
        opts.parseAndConfigure(parser, args);
        return opts;
    }

    private void configureEstimators() {
        if (statsProvider instanceof MetricStatisticsProvider) {
            MetricRegistry registry = ((MetricStatisticsProvider) statsProvider).getRegistry();
            progressReporter.setTraversalRateEstimator(new MetricRateEstimator("async", registry));
        }

        if (mongoConnection != null) {
            long nodesCount = mongoConnection.getDB().getCollection("nodes").count();
            progressReporter.setNodeCountEstimator((String basePath, Set<String> indexPaths) -> nodesCount);
            log.info("Estimated number of documents in Mongo are {}", nodesCount);
        }
    }
}
