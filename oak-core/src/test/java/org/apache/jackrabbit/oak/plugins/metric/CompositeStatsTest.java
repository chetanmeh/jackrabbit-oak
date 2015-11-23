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

package org.apache.jackrabbit.oak.plugins.metric;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CompositeStatsTest {
    private MetricRegistry registry = new MetricRegistry();
    private SimpleStats simpleStats = new SimpleStats(new AtomicLong());

    @Test
    public void counter() throws Exception {
        MetricCounterStats counter = new MetricCounterStats(registry.counter("test"));
        CounterStats counterStats = new CompositeStats(simpleStats, counter);

        counterStats.inc();
        assertEquals(1, simpleStats.getCount());
        assertEquals(1, counter.getCount());
        assertEquals(1, counterStats.getCount());

        counterStats.inc();
        counterStats.inc();
        assertEquals(3, simpleStats.getCount());

        counterStats.dec();
        assertEquals(2, simpleStats.getCount());
        assertEquals(2, counter.getCount());
    }

    @Test
    public void meter() throws Exception {
        Meter meter = registry.meter("test");
        MeterStats meterStats = new CompositeStats(simpleStats, new MetricMeterStats(meter));

        meterStats.mark();
        assertEquals(1, simpleStats.getCount());
        assertEquals(1, meter.getCount());

        meterStats.mark(5);
        assertEquals(6, simpleStats.getCount());
        assertEquals(6, meter.getCount());
    }

    @Test
    public void timer() throws Exception {
        Timer time = registry.timer("test");
        TimerStats timerStats = new CompositeStats(simpleStats, new MetricTimerStats(time));

        timerStats.update(100, TimeUnit.SECONDS);
        assertEquals(1, time.getCount());
        assertEquals(TimeUnit.SECONDS.toMillis(100), simpleStats.getCount());
    }
}
