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
import org.neo4j.driver.causal.Session;
import org.neo4j.driver.causal.Transaction;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.TypeSystem;

import java.util.Map;


public class InternalSession implements Session
{
    private final AccessMode defaultAccessMode;
    private final org.neo4j.driver.v1.Session v1Session;

    public InternalSession(AccessMode defaultAccessMode, Driver v1Driver)
    {
        this.defaultAccessMode = defaultAccessMode;
        switch (defaultAccessMode)
        {
            case READ:
                this.v1Session = v1Driver.session(org.neo4j.driver.v1.AccessMode.READ);
                break;
            case WRITE:
            default:
                this.v1Session = v1Driver.session(org.neo4j.driver.v1.AccessMode.WRITE);
        }
    }

    @Override
    public Transaction beginTransaction()
    {
        return null;
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode)
    {
        return null;
    }

    @Override
    public Transaction beginTransaction(String bookmark)
    {
        return beginTransaction(defaultAccessMode, bookmark);
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode, String bookmark)
    {
        switch (this.defaultAccessMode)
        {
            case READ:
                switch (accessMode)
                {
                    case READ:
                        return new InternalTransaction(this.v1Session, this.defaultAccessMode);
                    // these are guards against logic errors in caller ReadWriteSession: should never happen
                    case WRITE:
                    default:
                        throw new IllegalArgumentException("You cannot start a writable transaction with a read session");
                }
            case WRITE:
            default:
                switch (accessMode)
                {
                    // this is a guard against logic errors in caller ReadWriteSession: should never happen
                    case READ:
                        throw new IllegalArgumentException(
                                "You cannot start a read only transaction with a write session session");
                    case WRITE:
                    default:
                        return new InternalTransaction(this.v1Session, this.defaultAccessMode);
                }
        }
    }

    @Override
    public String lastBookmark()
    {
        // this is a local operation and cannot fail
        return v1Session.lastBookmark();
    }

    @Override
    public void reset()
    {
        // not sure what this done
        v1Session.reset();
    }

    @Override
    public boolean isOpen()
    {
        return v1Session.isOpen();
    }

    @Override
    public void close()
    {
        v1Session.close();
    }

    @Override
    public String server()
    {
        return v1Session.server();
    }

    @Override
    public StatementResult run(String statementTemplate, Value parameters)
    {
        return v1Session.run(statementTemplate, parameters);
    }

    @Override
    public StatementResult run(String statementTemplate, Map<String, Object> statementParameters)
    {
        return v1Session.run(statementTemplate, statementParameters);
    }

    @Override
    public StatementResult run(String statementTemplate, Record statementParameters)
    {
        return v1Session.run(statementTemplate, statementParameters);
    }

    @Override
    public StatementResult run(String statementTemplate)
    {
        return v1Session.run(statementTemplate);
    }

    @Override
    public StatementResult run(Statement statement)
    {
        return v1Session.run(statement);
    }

    @Override
    public TypeSystem typeSystem()
    {
        return v1Session.typeSystem();
    }
}
