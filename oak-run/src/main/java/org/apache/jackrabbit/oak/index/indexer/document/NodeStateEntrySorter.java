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
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryWriter.DELIMITER;

public class NodeStateEntrySorter {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final int DEFAULTMAXTEMPFILES = 1024;
    private final File nodeStateFile;
    private final File workDir;
    private final Comparator<String> comparator = NodeStateEntryPathComparator.INSTANCE;
    private final Charset charset = UTF_8;
    private File sortedFile;

    public NodeStateEntrySorter(File nodeStateFile, File workDir) {
        this(nodeStateFile, workDir, getSortedFileName(nodeStateFile));
    }

    public NodeStateEntrySorter(File nodeStateFile, File workDir, File sortedFile) {
        this.nodeStateFile = nodeStateFile;
        this.workDir = workDir;
        this.sortedFile = sortedFile;
    }

    public void sort() throws IOException {
        long memory = estimateAvailableMemory();
        log.info("Sorting with memory {}", humanReadableByteCount(memory));
        Stopwatch w = Stopwatch.createStarted();
        List<File> sortedFiles = ExternalSort.sortInBatch(nodeStateFile,
                comparator, //Comparator to use
                DEFAULTMAXTEMPFILES,
                memory,
                charset, //charset
                workDir,  //temp directory where intermediate files are created
                false
        );

        log.info("Batch sorting done in {} with {} files to merge", w, sortedFiles.size());
        Stopwatch w2 = Stopwatch.createStarted();

        ExternalSort.mergeSortedFiles(sortedFiles,
                sortedFile,
                comparator,
                charset,
                false
        );

        log.info("Merging of sorted files completed in {}", w2);
        log.info("Sorting completed in {}", w);
    }

    public File getSortedFile() {
        return sortedFile;
    }

    private static File getSortedFileName(File file) {
        return new File(file.getParentFile(), FilenameUtils.getBaseName(file.getName())+"-sorted.txt");
    }

    /**
     * This method calls the garbage collector and then returns the free
     * memory. This avoids problems with applications where the GC hasn't
     * reclaimed memory and reports no available memory.
     *
     * @return available memory
     */
    private static long estimateAvailableMemory() {
        System.gc();
        // http://stackoverflow.com/questions/12807797/java-get-available-memory
        Runtime r = Runtime.getRuntime();
        long allocatedMemory = r.totalMemory() - r.freeMemory();
        long presFreeMemory = r.maxMemory() - allocatedMemory;
        return presFreeMemory;
    }

    private static String getPath(String entry) {
        int indexOfPipe = entry.indexOf(DELIMITER);
        checkState(indexOfPipe > 0, "Invalid path entry [%s]", entry);
        return entry.substring(0, indexOfPipe);
    }

    private static int compare(Iterable<String> p1, Iterable<String> p2) {
        Iterator<String> i1 = p1.iterator();
        Iterator<String> i2 = p2.iterator();

        //Shorter paths come first i.e. first parent then children
        while (i1.hasNext() || i2.hasNext()) {
            if (!i1.hasNext()) {
                return -1;
            }
            if (!i2.hasNext()) {
                return 1;
            }
            int compare = i1.next().compareTo(i2.next());
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    enum NodeStateEntryPathComparator implements Comparator<String> {
        INSTANCE
        ;
        @Override
        public int compare(String e1, String e2) {
            String p1 = getPath(e1);
            String p2 = getPath(e2);
            return NodeStateEntrySorter.compare(elements(p1), elements(p2));
        }
    }
}
