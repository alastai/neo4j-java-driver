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

public class ReadWriteSession implements Session
{
    private final AccessMode accessMode;
    private final Consistency consistency;
    private final ToleranceForReplicationDelay toleranceForReplicationDelay;

    private final org.neo4j.driver.v1.Session readSession;
    private final org.neo4j.driver.v1.Session writeSession;
    private Transaction currentTransaction = null;

    public ReadWriteSession(org.neo4j.driver.v1.Driver v1Driver,
                            String bookmark,
                            AccessMode accessMode,
                            Consistency consistency,
                            ToleranceForReplicationDelay toleranceForReplicationDelay)
    {
        this.accessMode = accessMode;
        this.consistency = consistency;
        this.toleranceForReplicationDelay = toleranceForReplicationDelay;

        this.readSession = v1Driver.session(org.neo4j.driver.v1.AccessMode.READ);
        this.writeSession = v1Driver.session(org.neo4j.driver.v1.AccessMode.WRITE);
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction(AccessMode.WRITE);
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode)
    {
        switch (accessMode)
        {
            case READ:
                currentTransaction = new InternalTransaction(this.readSession, accessMode);
            case WRITE:
                currentTransaction = new InternalTransaction(this.writeSession, accessMode);
        }
        return currentTransaction;
    }

    @Override
    public Transaction beginTransaction(String bookmark)
    {
        return beginTransaction(AccessMode.WRITE, bookmark);
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode, String bookmark)
    {
        switch (accessMode)
        {
            case READ:
                currentTransaction = new InternalTransaction(this.readSession, accessMode, bookmark);
            case WRITE:
                currentTransaction = new InternalTransaction(this.writeSession, accessMode, bookmark);
        }
        return currentTransaction;
    }

    @Override
    public String lastBookmark()
    {
        switch (accessMode())
        {
            case READ:
                return readSession.lastBookmark();
            case WRITE:
            default:
                return writeSession.lastBookmark();
        }
    }

    @Override
    public void reset()
    {
        switch (accessMode())
        {
            case READ:
                readSession.reset();
            case WRITE:
            default:
                writeSession.reset();
        }
    }

    @Override
    public boolean isOpen()
    {
        switch (accessMode())
        {
            case READ:
                return readSession.isOpen();
            case WRITE:
            default:
                return writeSession.isOpen();
        }
    }

    @Override
    public void close()
    {
        switch (accessMode())
        {
            case READ:
                readSession.close();
            case WRITE:
            default:
                writeSession.close();
        }
    }

    @Override
    public String server()
    {
        switch (accessMode())
        {
            case READ:
                return readSession.server();
            case WRITE:
            default:
                return writeSession.server();
        }
    }

    @Override
    public StatementResult run(String statementTemplate, Value parameters)
    {
        switch (accessMode())
        {
            case READ:
                return readSession.run(statementTemplate, parameters);
            case WRITE:
            default:
                return writeSession.run(statementTemplate, parameters);
        }
    }

    @Override
    public StatementResult run(String statementTemplate, Map<String, Object> statementParameters)
    {
        switch (accessMode())
        {
            case READ:
                return readSession.run(statementTemplate, statementParameters);
            case WRITE:
            default:
                return writeSession.run(statementTemplate, statementParameters);
        }
    }

    @Override
    public StatementResult run(String statementTemplate, Record statementParameters)
    {
        switch (accessMode())
        {
            case READ:
                return readSession.run(statementTemplate, statementParameters);
            case WRITE:
            default:
                return writeSession.run(statementTemplate, statementParameters);
        }
    }

    @Override
    public StatementResult run(String statementTemplate)
    {
        switch (accessMode())
        {
            case READ:
                return readSession.run(statementTemplate);
            case WRITE:
            default:
                return writeSession.run(statementTemplate);
        }
    }

    @Override
    public StatementResult run(Statement statement)
    {
        switch (accessMode())
        {
            case READ:
                return readSession.run(statement);
            case WRITE:
            default:
                return writeSession.run(statement);
        }
    }

    @Override
    public TypeSystem typeSystem()
    {
        switch (accessMode())
        {
            case READ:
                return readSession.typeSystem();
            case WRITE:
            default:
                return writeSession.typeSystem();
        }
    }

    private AccessMode accessMode()
    {
        return currentTransaction.getAccessMode() == null ? AccessMode.WRITE : currentTransaction.getAccessMode();
    }
}
