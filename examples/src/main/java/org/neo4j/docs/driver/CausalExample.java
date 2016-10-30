/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.docs.driver;

import org.neo4j.driver.causal.AccessMode;
import org.neo4j.driver.causal.Consistency;
import org.neo4j.driver.causal.Driver;
import org.neo4j.driver.causal.GraphDatabase;
import org.neo4j.driver.causal.Session;
import org.neo4j.driver.causal.Transaction;
import org.neo4j.driver.v1.AuthTokens;

import java.util.UUID;

public class CausalExample
{
    public static String create(String label)
    {
        // imagine writing in first app server
        Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver1:7601",
                                             AuthTokens.basic("neo4j", "neo"));
        try (Session writeSession = driver.session(AccessMode.WRITE))
        {
            try (Transaction tx = writeSession.beginTransaction())
            {
                tx.run("CREATE (a:`" + label + "`)").consume();
                tx.success();
            }
            return writeSession.lastBookmark(); // it's always there, but you don't have to use it
        }
    }

    public static boolean match(String bookmark, String label)
    {
        // and “reading your own write” via a second app server
        Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver2:7687",
                                             AuthTokens.basic("neo4j", "neo"));
        boolean matched;
        try (Session readSession = driver.session(AccessMode.READ, bookmark))
        {

            try (Transaction tx = readSession.beginTransaction())
            {
                matched = tx.run("MATCH (a:`" + label + "`) RETURN a").hasNext();
                tx.success();
            }
        }
        return matched;
    }

    public static boolean createAndMatchWithCausallyConsistentSession(String label)
    {
        // imagine working in headless application that executes a stream of transactions
        Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver1:7687",
                AuthTokens.basic("neo4j", "neo"));

        boolean matched;

        try (Session causallyConsistentSession = driver.session()) // by default causally consistent and AccessMode.WRITE
        {
            try (Transaction tx = causallyConsistentSession.beginTransaction(AccessMode.WRITE))
            {
                tx.run("CREATE (a:`" + label + "`)").consume();
                tx.success();
            }

            try (Transaction tx = causallyConsistentSession.beginTransaction(AccessMode.READ))
            {
                matched = tx.run("MATCH (a:`" + label + "`) RETURN a").hasNext();
                tx.success();
            }
        }
        return matched;
    }

    public static boolean readAndRereadWithCausallyConsistentSession(String label)
    {
        // imagine working in headless application that executes a stream of transactions
        Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver1:7687",
                AuthTokens.basic("neo4j", "neo"));

        boolean matched;

        try (Session causallyConsistentSession = driver.session()) // by default causally consistent and AccessMode.WRITE
        {
            try (Transaction tx = causallyConsistentSession.beginTransaction(AccessMode.READ))
            {
                tx.run("CREATE (a:`" + label + "`)").consume();
                tx.success();
            }

            try (Transaction tx = causallyConsistentSession.beginTransaction(AccessMode.READ))
            {
                matched = tx.run("MATCH (a:`" + label + "`) RETURN a").hasNext();
                tx.success();
            }
        }
        return matched;
    }

    public static boolean readFromAnywhereInEventuallyConsistentSession(String label)
    {
        // imagine working in headless application that executes a stream of transactions
        Driver driver = GraphDatabase.driver("bolt+routing://com.customer.neocoreserver1:7687",
                AuthTokens.basic("neo4j", "neo"));

        boolean matched;

        try (Session causallyConsistentSession = driver.session(Consistency.EVENTUAL)) // AccessMode.WRITE
        {
            try (Transaction tx = causallyConsistentSession.beginTransaction(AccessMode.READ))
            {
                matched = tx.run("MATCH (a:`" + label + "`) RETURN a").hasNext();
                tx.success();
            }

            // very hard to emulate -- let's imagine that we are experiencing negative time travel

            try (Transaction tx = causallyConsistentSession.beginTransaction(AccessMode.READ))
            {
                matched = tx.run("MATCH (a:`" + label + "`) RETURN a").hasNext();
                tx.success();
            }
        }
        return matched;
    }

    public static void main(String[] args)
    {
        String label = UUID.randomUUID().toString();

        boolean matchReadAfterWriteInSeparateSessions = match(create(label), label);
        System.out.println("Separate READ and WRITE sessions results match: " + matchReadAfterWriteInSeparateSessions);

        boolean matchReadAfterWriteInSingleSession = createAndMatchWithCausallyConsistentSession(label);
        System.out.println("READ after WRITE in one session results match:  " + matchReadAfterWriteInSingleSession);

        boolean matchReadAfterReadInSingleSession = readAndRereadWithCausallyConsistentSession(label);
        System.out.println("READ after READ in one session results match:   " + matchReadAfterReadInSingleSession);

    }

}
