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
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

public class WriteSession implements BookmarkingSession
{
    private static final AccessMode DEFAULT_ACCESS_MODE = AccessMode.WRITE;

    private final Consistency consistency;
    private final ToleranceForReplicationDelay toleranceForReplicationDelay;

    private final org.neo4j.driver.v1.Driver v1Driver;
    private final org.neo4j.driver.v1.Session v1ReadSession;
    private final org.neo4j.driver.v1.Session v1WriteSession;

    private String bookmark;
    private Transaction currentTransaction = null;

    public WriteSession(Driver v1Driver,
                        Consistency consistency,
                        ToleranceForReplicationDelay toleranceForReplicationDelay,
                        String bookmark)
    {
        this.consistency = consistency;
        this.toleranceForReplicationDelay = toleranceForReplicationDelay;

        this.v1Driver = v1Driver;
        this.v1ReadSession = v1Driver.session(org.neo4j.driver.v1.AccessMode.READ);
        this.v1WriteSession = v1Driver.session(org.neo4j.driver.v1.AccessMode.WRITE);

        this.bookmark = bookmark; // May be null: in which case bookmarking may be turned off in this session,
        // but check the specified consistency level in that case, as that is authoritative:
        // this may be the start of a causally consistent chain.

        // If a Session is initialized with a bookmark, then bookmarking is turned on,
        // which means that V1 transactions are fed bookmarks, and bookmarks are extracted
        // from V1 sessions. Causal sessions are fed bookmarks and yield bookmarks
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction(AccessMode.WRITE);
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode)
    {
        switch (this.consistency)
        {
            case CAUSAL:
                return new InternalTransaction(this, accessMode, this.bookmark);
            case EVENTUAL:
            default:
                return new InternalTransaction(this, accessMode);
        }
    }

    @Override
    public String lastBookmark()
    {
        return this.bookmark;
    }

    @Override
    public boolean isOpen()
    {
        return true; // is this meaningful at this level?
    }

    @Override
    public void close()
    {
        v1ReadSession.close();
        v1WriteSession.close();
    }

    @Override
    public void setBookmark(String bookmark)
    {
        this.bookmark = bookmark;
    }

    @Override
    public Session v1Session(AccessMode accessMode)
    {
        switch (accessMode)
        {
            case READ:
            {
                return v1ReadSession;
            }
            case WRITE:
            default:
            {
                return v1WriteSession;
            }
        }
    }

    @Override
    public void refreshV1Session(AccessMode accessMode) throws ServiceUnavailableException
    {
        try
        {
            switch (accessMode)
            {
                case READ:
                {
                    this.v1Driver.session(org.neo4j.driver.v1.AccessMode.READ);
                }
                case WRITE:
                default:
                {
                    this.v1Driver.session(org.neo4j.driver.v1.AccessMode.WRITE);
                }
            }
        }
        catch (ServiceUnavailableException serviceUnavailableException)
        {
            // if we get here, then the driver is out of the water
            v1Driver.close(); //
            throw serviceUnavailableException; // app gets the fatal error and should give up at this point
        }
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
    public AccessMode defaultTransactionAccessMode()
    {
        return DEFAULT_ACCESS_MODE;
    }
}
