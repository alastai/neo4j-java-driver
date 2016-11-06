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

public class ReadSession implements BookmarkingSession
{
    private static final AccessMode DEFAULT_ACCESS_MODE = AccessMode.READ;

    private final Consistency consistency;
    private final ToleranceForReplicationDelay toleranceForReplicationDelay;

    private final org.neo4j.driver.v1.Driver v1Driver;
    private org.neo4j.driver.v1.Session v1ReadSession;

    private String bookmark;
    private Transaction currentTransaction = null;

    public ReadSession(Driver v1Driver,
                       Consistency consistency,
                       ToleranceForReplicationDelay toleranceForReplicationDelay,
                       String bookmark)
    {
        this.consistency = consistency;
        this.toleranceForReplicationDelay = toleranceForReplicationDelay;

        this.v1Driver = v1Driver;
        this.v1ReadSession = this.v1Driver.session(org.neo4j.driver.v1.AccessMode.READ);

        this.bookmark = bookmark; // May be null: in which case bookmarking may be turned off in this session,
        // but check the specified consistency level, as that is authoritative: this may be the
        // start of a causally consistent chain.

        // If a Session is initialized with a bookmark, then bookmarking is turned on,
        // which means that V1 transactions are fed bookmarks, and bookmarks are extracted
        // from V1 sessions. Causal sessions are fed bookmarks and yield bookmarks
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction(AccessMode.READ);
    }

    @Override
    public Transaction beginTransaction(AccessMode accessMode)
    {
        switch (this.consistency)
        {
            case CAUSAL:
                return new InternalTransaction(this, accessMode, bookmark);
            case EVENTUAL:
            default:
                return new InternalTransaction(this, accessMode);
        }
    }

    @Override
    public String lastBookmark()
    {
        return this.bookmark; // may be null, may be the initial value, may be the value updated by a transaction close
    }

    @Override
    public boolean isOpen()
    {
        return v1ReadSession.isOpen();
    }

    @Override
    public void close()
    {
        v1ReadSession.close();
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

    @Override
    public void setBookmark(String bookmark)
    {
        this.bookmark = bookmark; // invoked at the point of commit of the v1 transaction, by InternalTransaction
    }

    @Override
    public Session v1Session(AccessMode accessMode)
    {
        return this.v1ReadSession;
    }

    @Override
    public void refreshV1Session(AccessMode accessMode) throws ServiceUnavailableException
    {
        try
        {
            this.v1ReadSession = this.v1Driver.session(org.neo4j.driver.v1.AccessMode.READ);
        }
        catch (ServiceUnavailableException serviceUnavailableException)
        {
            // if we get here, then the driver is out of the water
            v1Driver.close(); //
            throw serviceUnavailableException; // app gets the fatal error and should give up at this point
        }

    }
}
