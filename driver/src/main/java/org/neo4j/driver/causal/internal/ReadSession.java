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
package org.neo4j.driver.causal.internal;

import org.neo4j.driver.causal.AccessMode;
import org.neo4j.driver.causal.Consistency;
import org.neo4j.driver.causal.Session;
import org.neo4j.driver.causal.ToleranceForReplicationDelay;
import org.neo4j.driver.causal.Transaction;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.TypeSystem;

import java.util.Map;

public class ReadSession implements Session
{
    private final AccessMode accessMode;
    private final Consistency consistency;
    private final ToleranceForReplicationDelay toleranceForReplicationDelay;

    private final org.neo4j.driver.v1.Session readSession;
    private Transaction currentTransaction = null;

    public ReadSession(org.neo4j.driver.v1.Driver v1Driver,
                       String bookmark,
                       AccessMode accessMode,
                       Consistency consistency,
                       ToleranceForReplicationDelay toleranceForReplicationDelay)
    {
        this.accessMode = accessMode;
        this.consistency = consistency;
        this.toleranceForReplicationDelay = toleranceForReplicationDelay;

        this.readSession = v1Driver.session(org.neo4j.driver.v1.AccessMode.READ);
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction(AccessMode.WRITE);
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode)
    {
        return currentTransaction = new InternalTransaction(this.readSession, accessMode);
    }

    @Override
    public Transaction beginTransaction(String bookmark)
    {
        return beginTransaction(AccessMode.WRITE, bookmark);
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode, String bookmark)
    {
        return currentTransaction = new InternalTransaction(this.readSession, accessMode, bookmark);
    }

    @Override
    public String lastBookmark()
    {
        return readSession.lastBookmark();
    }

    @Override
    public void reset()
    {
        readSession.reset();
    }

    @Override
    public boolean isOpen()
    {
        return readSession.isOpen();
    }

    @Override
    public void close()
    {
        readSession.close();
    }

    @Override
    public String server()
    {
        return readSession.server();
    }

    @Override
    public StatementResult run(String statementTemplate, Value parameters)
    {
        return readSession.run(statementTemplate, parameters);
    }

    @Override
    public StatementResult run(String statementTemplate, Map<String, Object> statementParameters)
    {
        return readSession.run(statementTemplate, statementParameters);
    }

    @Override
    public StatementResult run(String statementTemplate, Record statementParameters)
    {
        return readSession.run(statementTemplate, statementParameters);
    }

    @Override
    public StatementResult run(String statementTemplate)
    {
        return readSession.run(statementTemplate);
    }

    @Override
    public StatementResult run(Statement statement)
    {
        return readSession.run(statement);
    }

    @Override
    public TypeSystem typeSystem()
    {
        return readSession.typeSystem();
    }

    private AccessMode accessMode()
    {
        return currentTransaction.getAccessMode() == null ? AccessMode.WRITE : currentTransaction.getAccessMode();
    }
}
