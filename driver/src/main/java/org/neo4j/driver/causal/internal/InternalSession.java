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
package org.neo4j.driver.causal.internal;

import org.neo4j.driver.causal.Consistency;
import org.neo4j.driver.causal.NotCommittedException;
import org.neo4j.driver.causal.ToleranceForReplicationDelay;
import org.neo4j.driver.causal.Transaction;
import org.neo4j.driver.causal.UnitOfWorkRetryParameters;
import org.neo4j.driver.causal.UnknownTransactionOutcomeException;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import java.util.function.Function;

import static java.lang.String.format;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;

public class InternalSession implements BookmarkingSession
{
    private final Consistency consistency;
    private final ToleranceForReplicationDelay toleranceForReplicationDelay;
    private final UnitOfWorkRetryParameters defaultUnitOfWorkParameters;

    private final org.neo4j.driver.v1.Driver v1Driver;
    private org.neo4j.driver.v1.Session v1ReadSession;
    private org.neo4j.driver.v1.Session v1WriteSession;

    private String bookmark;
    private Transaction currentTransaction = null;

    public InternalSession(Driver v1Driver,
                           Consistency consistency,
                           ToleranceForReplicationDelay toleranceForReplicationDelay,
                           UnitOfWorkRetryParameters defaultUnitOfWorkParameters,
                           String bookmark)
    {
        this.consistency = consistency;
        this.toleranceForReplicationDelay = toleranceForReplicationDelay;
        this.defaultUnitOfWorkParameters = defaultUnitOfWorkParameters;

        this.v1Driver = v1Driver;

        this.bookmark = bookmark; // May be null: in which case bookmarking may be turned off in this session,
        // but check the specified consistency level in that case, as that is authoritative:
        // this may be the start of a causally consistent chain.

        // If a InternalSession is initialized with a bookmark, then bookmarking is turned on,
        // which means that V1 transactions are fed bookmarks, and bookmarks are extracted
        // from V1 sessions. Causal sessions are fed bookmarks and yield bookmarks.
    }

    private Transaction beginTransaction()
    {
        return beginTransaction(WRITE);
    }

    private Transaction beginTransaction(AccessMode accessMode)
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
    public <T> T readUnitOfWork(Function<Transaction, T> unitOfWork) throws NotCommittedException, ServiceUnavailableException
    {
        return readUnitOfWork(unitOfWork, this.defaultUnitOfWorkParameters);
    }

    @Override
    public <T> T readUnitOfWork(Function<Transaction, T> unitOfWork, UnitOfWorkRetryParameters unitOfWorkRetryParameters) throws NotCommittedException, ServiceUnavailableException
    {
        return unitOfWork(AccessMode.READ, unitOfWork, unitOfWorkRetryParameters);
    }

    @Override
    public <T> T writeUnitOfWork(Function<Transaction, T> unitOfWork) throws NotCommittedException, ServiceUnavailableException
    {
        return writeUnitOfWork(unitOfWork, this.defaultUnitOfWorkParameters);
    }

    @Override
    public <T> T writeUnitOfWork(Function<Transaction, T> unitOfWork, UnitOfWorkRetryParameters unitOfWorkRetryParameters) throws NotCommittedException, ServiceUnavailableException
    {
        return unitOfWork(AccessMode.WRITE, unitOfWork, unitOfWorkRetryParameters);
    }

    private <T> T unitOfWork(AccessMode accessMode, Function<Transaction, T> unitOfWork, UnitOfWorkRetryParameters unitOfWorkRetryParameters)
            throws UnknownTransactionOutcomeException, ServiceUnavailableException
    {
        int remaining = unitOfWorkRetryParameters.attempts();
        while (remaining > 0)
        {
            boolean failed = false;
            Transaction transaction = beginTransaction(accessMode);
            try
            {
                T result = unitOfWork.apply(transaction);
                transaction.success();
                return result;
            }
            catch (SessionExpiredException e)
            {
                failed = true;
                transaction.failure();
                remaining -= 1;
            }
            finally
            {
                if (failed)
                {
                    try
                    {
                        transaction.close();
                    }
                    catch (Exception exception)
                    {
                        // ignore errors if we've already failed as
                        // we already know this connection is problematic
                    }
                }
                else
                {
                    transaction.close();
                }
            }
            try
            {
                Thread.sleep(unitOfWorkRetryParameters.pauseMillis());
            }
            catch (InterruptedException interrruptedException)
            {
                Thread.currentThread().interrupt();
            }
        }
        throw new UnknownTransactionOutcomeException(format("Unable to commit transaction after %d attempts", unitOfWorkRetryParameters.attempts()));
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
        v1WriteSession.close(); // TODO check if these are idempotent: the two sessions may be the same thing
    }

    @Override
    public void setBookmark(String bookmark)
    {
        this.bookmark = bookmark;
    }

    @Override
    public org.neo4j.driver.v1.Session v1Session(AccessMode accessMode)
    {
        switch (accessMode) // make sure the right session is doled out depending on transaction access mode
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
                    this.v1ReadSession = this.v1Driver.session(READ);
                }
                case WRITE:
                default:
                {
                    this.v1WriteSession = this.v1Driver.session(WRITE);
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
}
