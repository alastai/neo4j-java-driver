/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.docs.driver;

import org.neo4j.driver.causal.Consistency;
import org.neo4j.driver.causal.Driver;
import org.neo4j.driver.causal.GraphDatabase;
import org.neo4j.driver.causal.Session;
import org.neo4j.driver.causal.ToleranceForReplicationDelay;
import org.neo4j.driver.causal.UnknownTransactionOutcomeException;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import java.util.UUID;

public class CausalExample
{
    public static String create(String label)
    {
        // imagine writing in first app server
        Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver1:7601",
                AuthTokens.basic("neo4j", "neo"));
        try (Session causallyConsistentSession = driver.session())
        {
            Iterable<Record> records;

            records = causallyConsistentSession.writeUnitOfWork(transaction -> transaction.run("CREATE (a:`" + label + "`) RETURN a").list());
            return causallyConsistentSession.lastBookmark(); // it's always there, but you don't have to use it
        }
        catch (ServiceUnavailableException serviceUnavailableException)
        {
            // Driver is out of the water

            // this is the only expected exception: any cluster server failure or cluster membership/role change should be
            // handled automagically

            // restart app? modify config? tell user agent that the service isn't available? etc
        }
        catch (UnknownTransactionOutcomeException unknownTransactionOutcome)
        {
            // now what?
        }
        return null; // for now
    }

    public static void match(String bookmark, String label)
    {
        // and “reading your own writeUnitOfWork” via a second app server
        Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver2:7687",
                AuthTokens.basic("neo4j", "neo"));
        try (Session causallyConsistentSession = driver.session(bookmark))
        {
            Iterable<Record> records;

            records = causallyConsistentSession.readUnitOfWork(transaction -> transaction.run("MATCH (a:`" + label + "`) RETURN a").list());
        }
        catch (ServiceUnavailableException serviceUnavailableException)
        {
            // Driver is out of the water

            // this is the only expected exception: any cluster server failure or cluster membership/role change should be
            // handled automagically

            // restart app? modify config? tell user agent that the service isn't available? etc
        }
        catch (UnknownTransactionOutcomeException unknownTransactionOutcome)
        {
            // now what?
        }
    }

    public static void createAndMatchWithCausallyConsistentSession(String label)
    {
        // imagine working in headless application that executes a stream of transactions
        Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver1:7687",
                AuthTokens.basic("neo4j", "neo"));

        try (Session causallyConsistentSession = driver.session()) // by default causally consistent
        {
            Iterable<Record> records;

            records = causallyConsistentSession.writeUnitOfWork(transaction -> transaction.run("CREATE (a:`" + label + "`) RETURN a").list());
            records = causallyConsistentSession.readUnitOfWork(transaction -> transaction.run("MATCH (a:`" + label + "`) RETURN a").list());
        }
        catch (ServiceUnavailableException serviceUnavailableException)
        {
            // Driver is out of the water

            // this is the only expected exception: any cluster server failure or cluster membership/role change should be
            // handled automagically

            // restart app? modify config? tell user agent that the service isn't available? etc
        }
        catch (UnknownTransactionOutcomeException unknownTransactionOutcome)
        {
            // now what?
        }
    }

    public static void readAndRereadWithCausallyConsistentSession(String label)
    {
        // imagine working in headless application that executes a stream of transactions

        try
        {
            Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver1:7687",
                    AuthTokens.basic("neo4j", "neo"));

            try (Session causallyConsistentSession = driver.session()) // by default causally consistent
            {
                Iterable<Record> records;

                records = causallyConsistentSession.writeUnitOfWork(transaction -> transaction.run("CREATE (a:`" + label + "`) RETURN a").list());
                records = causallyConsistentSession.readUnitOfWork(transaction -> transaction.run("MATCH (a:`" + label + "`) RETURN a").list());
                records = causallyConsistentSession.readUnitOfWork(transaction -> transaction.run("MATCH (a:`" + label + "`) RETURN a").list());
            }
        }
        catch (ServiceUnavailableException serviceUnavailableException)
        {
            // Driver is out of the water

            // this is the only expected exception: any cluster server failure or cluster membership/role change should be
            // handled automagically

            // restart app? modify config? tell user agent that the service isn't available? etc
        }
        catch (UnknownTransactionOutcomeException unknownTransactionOutcome)
        {
            // now what?
        }
    }

    public static void readFromAnywhereInEventuallyConsistentSession(String label)
    {
        // imagine working in headless application that executes a stream of transactions
        // or in Javascript in a serverless application

        Driver driver = GraphDatabase.driver(AuthTokens.basic("neo4j", "neo"),
                "bolt+routing://com.customer.neocoreserver1:7687",
                "bolt+routing://com.customer.neocoreserver2:7687",
                "bolt+routing://com.customer.neocoreserver3:7687");

        // specifiying ToleranceForReplicationDelay.HIGH near-guarantees we will go to a Read Replica, if available, for readUnitOfWork transactions

        try (Session eventuallyConsistentSession = driver.session(Consistency.EVENTUAL, ToleranceForReplicationDelay.HIGH))
        {
            Iterable<Record> records;

            records = eventuallyConsistentSession.writeUnitOfWork(transaction -> transaction.run("CREATE (a:`" + label + "`) RETURN a").list());

            // readUnitOfWork from a Read Replica -- likely to experience negative time travel

            records = eventuallyConsistentSession.readUnitOfWork(transaction -> transaction.run("MATCH (a:`" + label + "`) RETURN a").list());
        }
        catch (ServiceUnavailableException serviceUnavailableException)
        {
            // Driver is out of the water

            // this is the only expected exception: any cluster server failure or cluster membership/role change should be
            // handled automagically

            // restart app? modify config? tell user agent that the service isn't available? etc
        }
        catch (UnknownTransactionOutcomeException unknownTransactionOutcome)
        {
            // now what?
        }
    }

    public static void main(String[] args)
    {
        String label = UUID.randomUUID().toString();

        match(create(label), label);
        createAndMatchWithCausallyConsistentSession(label);
        readAndRereadWithCausallyConsistentSession(label);
        readFromAnywhereInEventuallyConsistentSession(label);
    }

}
