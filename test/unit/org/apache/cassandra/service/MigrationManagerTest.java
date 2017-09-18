/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Multimaps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;


public class MigrationManagerTest
{

    private static VersionedValue.VersionedValueFactory vvFact;
    private Random random;
    private MigrationManager migrationManager;
    private TestDebuggableScheduledThreadPoolExecutor testPool;
    private ArrayList<Runnable> scheduledcommands;
    private List<Runnable> commandLog;
    private LocalAwareExecutorService testStage;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.loadSchema();
        vvFact = new VersionedValue.VersionedValueFactory(RandomPartitioner.instance);
    }

    @Before
    public void setup() throws InterruptedException
    {

        random = new Random(10);
        migrationManager = new MigrationManager(Multimaps.newSetMultimap(new ConcurrentHashMap<>(), ConcurrentHashMap::newKeySet),
                                                new ConcurrentHashMap<>(), () -> random, inetAddress -> true);
        scheduledcommands = new ArrayList<>();
        testPool = new TestDebuggableScheduledThreadPoolExecutor("migrationtestpool", scheduledcommands);
        commandLog = new ArrayList<>();
        testStage = new TestDebuggableThreadPoolExecutor("migrationteststage", Thread.NORM_PRIORITY, commandLog);
    }

    @Test
    public void testUpdateSchemaSame() throws UnknownHostException, InterruptedException
    {
        Schema.instance.updateVersion();
        List<UUID> schemaVersions = Collections.singletonList(Schema.instance.getVersion());
        List<EndpointState> endpointStates = endpointStatesFromSchemVersions(random, 100, schemaVersions);

        List<Thread> threads = addMigrationsConcurrently(endpointStates, migrationManager, 5);

        for (int i = 0; i < 3; i++)
        {
            migrationManager.schemaVersionConvergence(testPool, testStage);
        }
        for (Thread t : threads)
        {
            t.join();
        }
        migrationManager.schemaVersionConvergence(testPool, testStage);
        assertEquals(0, commandLog.size());
    }

    @Test
    public void testUpdateSchemaOnce() throws UnknownHostException, InterruptedException
    {
        Schema.instance.updateVersion();
        List<UUID> schemaVersions = Collections.singletonList(UUID.randomUUID());
        List<EndpointState> endpointStates = endpointStatesFromSchemVersions(random, 100, schemaVersions);


        List<Thread> threads = addMigrationsConcurrently(endpointStates, migrationManager, 5);
        migrationManager.schemaVersionConvergence(testPool, testStage);
        for (Thread t : threads)
        {
            t.join();
        }
        assertEquals(1, commandLog.size());
    }

    @Test
    public void testUpdateSchemaConcurrently() throws UnknownHostException
    {

        List<EndpointState> endpointStates = generateEndpointStates(random, 100, 1000);

        addMigrationsConcurrently(endpointStates, migrationManager, 10);

        int numChecks = 12;
        for (int i = 0; i < numChecks; i++)
        {
            migrationManager.schemaVersionConvergence(testPool, testStage);
        }
        assertEquals(numChecks, commandLog.size());
    }

    private List<Thread> addMigrationsConcurrently(List<EndpointState> endpointStates, MigrationManager migrationManager, final int numThreads)
    {
        List<Thread> threads = IntStream.range(0, numThreads)
                                        .mapToObj(idx -> new Thread(getInsertFunc(endpointStates, migrationManager, numThreads, idx)))
                                        .collect(Collectors.toList());
        threads.forEach(Thread::start);
        return threads;
    }

    private List<EndpointState> generateEndpointStates(Random random, final int numSchemaVersions, final int numEndpointStates) throws UnknownHostException
    {
        List<UUID> uuids = IntStream.range(0, numSchemaVersions).mapToObj(i -> new UUID(random.nextLong(), random.nextLong())).collect(Collectors.toList());
        return endpointStatesFromSchemVersions(random, numEndpointStates, uuids);
    }

    private List<EndpointState> endpointStatesFromSchemVersions(Random random, int numEndpointStates, List<UUID> schemaVersions) throws UnknownHostException
    {
        List<EndpointState> endpointStates = IntStream.range(0, numEndpointStates).mapToObj(x ->
                                                                                            generateEndpointState(schemaVersions.get(random.nextInt(schemaVersions.size()))))
                                                      .collect(Collectors.toList());


        for (int i = 0; i < endpointStates.size(); i++)
        {
            MessagingService.instance().setVersion(idxToAddress(i), MessagingService.current_version);
        }
        return endpointStates;
    }

    private Runnable getInsertFunc(List<EndpointState> endpointStates, MigrationManager migrationManager, int numThreads, int idx)
    {
        return () -> IntStream.range((endpointStates.size() / numThreads) * idx, (endpointStates.size() / numThreads) * (idx + 1))
                              .forEach(endpointIdx ->
                                       {
                                           try
                                           {
                                               migrationManager.updateSchemaVersionMap(idxToAddress(endpointIdx), endpointStates.get(endpointIdx));
                                           }
                                           catch (UnknownHostException e)
                                           {
                                               throw new AssertionError(e);
                                           }
                                       });
    }

    private InetAddress idxToAddress(int endpointIdx) throws UnknownHostException
    {
        return InetAddress.getByAddress(ByteBufferUtil.bytes(endpointIdx).array());
    }

    final static class TestDebuggableScheduledThreadPoolExecutor extends DebuggableScheduledThreadPoolExecutor
    {

        private final List<Runnable> commandLog;

        TestDebuggableScheduledThreadPoolExecutor(String threadPoolName, List<Runnable> commandLog)
        {
            super(threadPoolName);
            this.commandLog = commandLog;
        }

        public void execute(Runnable command)
        {
            commandLog.add(command);
            super.execute(command);
        }
    }

    final static class TestDebuggableThreadPoolExecutor extends DebuggableThreadPoolExecutor
    {

        private final List<Runnable> commandLog;

        TestDebuggableThreadPoolExecutor(String threadPoolName, int priority, List<Runnable> commandLog)
        {
            super(threadPoolName, priority);
            this.commandLog = commandLog;
        }

        public Future<?> submit(Runnable task)
        {
            commandLog.add(task);
            return super.submit(task);
        }
    }

    private EndpointState generateEndpointState(UUID uuid)
    {

        EndpointState endpointState = org.apache.cassandra.gms.SerializationsTest.getEndpointState(100, 100);
        endpointState.addApplicationState(ApplicationState.SCHEMA, vvFact.schema(uuid));
        endpointState.addApplicationState(ApplicationState.SCHEMA, vvFact.schema(uuid));
        return endpointState;
    }
}
