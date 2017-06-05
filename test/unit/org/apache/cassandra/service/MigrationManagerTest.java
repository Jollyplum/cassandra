/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertTrue;

/**
 * Test for "Migration task " (CASSANDRA-12281).
 */
@RunWith(BMUnitRunner.class)
public class MigrationManagerTest
{
    private static final ExecutorService executor = Executors.newFixedThreadPool(5);
    private static AtomicInteger tasks;

    /*
     * NOTE: the tests above uses RandomPartitioner, which is not the default
     * test partitioner. Changing the partitioner should be done before
     * loading the schema as loading the schema trigger the write of sstables.
     * So instead of extending SchemaLoader, we call it's method below.
     */
    @BeforeClass
    public static void setup() throws Exception
    {

        System.setProperty("cassandra.migration.max_tasks_in_flight", "1");
        System.setProperty("cassandra.migration_task_wait_in_seconds", "10");
        System.setProperty("cassandra.migration.task_limit_expiry_time_ms", "10");
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        DatabaseDescriptor.setDaemonInitialized();
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();

        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddress> hosts = new ArrayList<>();

        // create a ring of 2 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, new ArrayList<>(), 3);

        Schema.instance.updateVersion();
        for (InetAddress endpoint : hosts)
        {
            MessagingService.instance().setVersion(endpoint, MessagingService.current_version);
            Map<ApplicationState, VersionedValue> stateMap = new EnumMap<>(ApplicationState.class);
            stateMap.put(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(UUID.randomUUID()));
            Gossiper.instance.getEndpointStateForEndpoint(endpoint).addApplicationStates(stateMap);
        }
    }

    @AfterClass
    public static void tearDown()
    {
        System.clearProperty("cassandra.migration.max_tasks_in_flight");
        System.clearProperty("cassandra.migration_task_wait_in_seconds");
        System.clearProperty("cassandra.migration.task_limit_expiry_time_ms");
    }

    private static EndpointState constructEndpointState(UUID schemaVersion)
    {
        DataOutputBuffer out = new DataOutputBuffer();
        try
        {
            EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(InetAddress.getByName("127.0.0.2"));
            EndpointState.serializer.serialize(endpointState, out, MessagingService.current_version);
            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            EndpointState newEndpointState = EndpointState.serializer.deserialize(in, MessagingService.current_version);
            newEndpointState.addApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(schemaVersion));
            return newEndpointState;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

    }

    @Test
    @BMRule(name = "Count the number of schema requests by MigrationManager",
            targetClass = "MigrationTask",
            targetMethod = "maybeSubmitMigrationTask",
            targetLocation = "AT INVOKE org.apache.cassandra.net.MessagingService.sendRRWithFailure",
            action = " org.apache.cassandra.service.MigrationManagerTest.incrementCount()")
    public void testMigrationLimit() throws UnknownHostException, InterruptedException
    {
        tasks = new AtomicInteger(0);

        InetAddress byName = InetAddress.getByName("127.0.0.2");
        IntStream.range(0, 100).mapToObj(y -> UUID.randomUUID()).forEach(uuid ->
                                                                         executor.submit(() ->
                                                                                         MigrationManager.scheduleSchemaPull(byName, constructEndpointState(uuid))));
        MigrationManager.waitUntilReadyForBootstrap();

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        Thread.sleep(20);
        MigrationManager.scheduleSchemaPull(byName, constructEndpointState(UUID.randomUUID()));

        for (int i=0; i<10; i++)
        {
            if (MigrationManager.numTasksInFlight.get() < 2)
            {
                break;
            }
            Thread.sleep(20);
        }
        int tasksInFlightNow = MigrationManager.numTasksInFlight.get();
        assertTrue(String.format("tasksInFlightNow: %s was not less than 2", tasksInFlightNow), tasksInFlightNow < 2);

        assertTrue(String.format("Expected more requests for schema to be sent, tasks wasn't > 0: %d", tasks.get()), tasks.get() > 0);
        assertTrue(String.format("Expected fewer requests for schema due to task limit, tasks wasn't < 50: %d", tasks.get()), tasks.get() < 50);
    }

    @SuppressWarnings("unused")
    public static void incrementCount()
    {
        tasks.incrementAndGet();
    }
}
