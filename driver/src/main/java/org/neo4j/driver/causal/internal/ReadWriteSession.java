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
import org.neo4j.driver.causal.ToleranceForReplicationDelay;
import org.neo4j.driver.causal.Transaction;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.TypeSystem;

import java.util.Map;

public class ReadWriteSession implements BookmarkingSession
{
    private String bookmark;
    private final AccessMode accessMode;
    private final Consistency consistency;
    private final ToleranceForReplicationDelay toleranceForReplicationDelay;

    private final org.neo4j.driver.v1.Session v1ReadSession;
    private final org.neo4j.driver.v1.Session v1WriteSession;
    private Transaction currentTransaction = null;

    public ReadWriteSession(Driver v1Driver,
                            AccessMode accessMode, 
                            Consistency consistency, 
                            ToleranceForReplicationDelay toleranceForReplicationDelay, 
                            String bookmark)
    {
        this.bookmark = bookmark; // May be null: in which case bookmarking may be turned off in this session,
                                  // but check the specified consistency level in that case, as that is authoritative:
                                  // this may be the start of a causally consistent chain.

                                  // If a Session is initialized with a bookmark, then bookmarking is turned on,
                                  // which means that V1 transactions are fed bookmarks, and bookmarks are extracted
                                  // from V1 sessions. Causal sessions are fed bookmarks and yield bookmarks
        this.accessMode = accessMode;
        this.consistency = consistency;
        this.toleranceForReplicationDelay = toleranceForReplicationDelay;

        this.v1ReadSession = v1Driver.session(org.neo4j.driver.v1.AccessMode.READ);
        this.v1WriteSession = v1Driver.session(org.neo4j.driver.v1.AccessMode.WRITE);
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction(AccessMode.WRITE);
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode)
    {
        org.neo4j.driver.v1.Session v1Session;

        switch (accessMode)
        {
            case READ:
                v1Session = this.v1ReadSession;
            case WRITE:
            default:
                v1Session = this.v1WriteSession;
        }

        switch (this.consistency)
        {
            case CAUSAL:
                return new InternalTransaction(this, v1Session, accessMode, this.bookmark);
            case EVENTUAL:
            default:
                return new InternalTransaction(this, v1Session, accessMode);
        }
    }

    @Override
    public String lastBookmark()
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.lastBookmark();
            case WRITE:
            default:
                return v1WriteSession.lastBookmark();
        }
    }

    @Override
    public void reset()
    {
        switch (this.accessMode)
        {
            case READ:
                v1ReadSession.reset();
            case WRITE:
            default:
                v1WriteSession.reset();
        }
    }

    @Override
    public boolean isOpen()
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.isOpen();
            case WRITE:
            default:
                return v1WriteSession.isOpen();
        }
    }

    @Override
    public void close()
    {
        switch (this.accessMode)
        {
            case READ:
                v1ReadSession.close();
            case WRITE:
            default:
                v1WriteSession.close();
        }
    }

    @Override
    public String server()
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.server();
            case WRITE:
            default:
                return v1WriteSession.server();
        }
    }

    @Override
    public StatementResult run(String statementTemplate, Value parameters)
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.run(statementTemplate, parameters);
            case WRITE:
            default:
                return v1WriteSession.run(statementTemplate, parameters);
        }
    }

    @Override
    public StatementResult run(String statementTemplate, Map<String, Object> statementParameters)
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.run(statementTemplate, statementParameters);
            case WRITE:
            default:
                return v1WriteSession.run(statementTemplate, statementParameters);
        }
    }

    @Override
    public StatementResult run(String statementTemplate, Record statementParameters)
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.run(statementTemplate, statementParameters);
            case WRITE:
            default:
                return v1WriteSession.run(statementTemplate, statementParameters);
        }
    }

    @Override
    public StatementResult run(String statementTemplate)
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.run(statementTemplate);
            case WRITE:
            default:
                return v1WriteSession.run(statementTemplate);
        }
    }

    @Override
    public StatementResult run(Statement statement)
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.run(statement);
            case WRITE:
            default:
                return v1WriteSession.run(statement);
        }
    }

    @Override
    public TypeSystem typeSystem()
    {
        switch (this.accessMode)
        {
            case READ:
                return v1ReadSession.typeSystem();
            case WRITE:
            default:
                return v1WriteSession.typeSystem();
        }
    }

    @Override
    public void setBookmark(String bookmark)
    {
        this.bookmark = bookmark;
    }

    @Override
    public Consistency consistency()
    {
        return this.consistency;
    }

    @Override
    public ToleranceForReplicationDelay toleranceForReplicationDelay()
    {
        return this.toleranceForReplicationDelay;
    }

    @Override
    public AccessMode accessMode()
    {
        return this.accessMode;
    }
}
