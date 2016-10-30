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
import org.neo4j.driver.causal.Outcome;
import org.neo4j.driver.causal.Transaction;
import org.neo4j.driver.causal.TransactionState;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.TypeSystem;

import java.util.Map;

public class InternalTransaction implements Transaction
{
    private final BookmarkingSession parentSession;
    private final AccessMode accessMode;
    private final org.neo4j.driver.v1.Session v1Session;
    private final org.neo4j.driver.v1.Transaction v1Transaction;
    private Outcome outcome;

    public AccessMode getAccessMode()
    {
        // doesn't exist ** return v1Session.getAccessMode();
        return this.accessMode;
    }

    public InternalTransaction(BookmarkingSession parentSession, org.neo4j.driver.v1.Session v1Session, AccessMode accessMode)
    {
        this.parentSession = parentSession;
        this.v1Session = v1Session;
        this.accessMode = accessMode;

        this.v1Transaction = this.v1Session.beginTransaction(); // will be READ for a READ session, and WRITE for a WRITE session
    }

    public InternalTransaction(BookmarkingSession parentSession, org.neo4j.driver.v1.Session v1Session, AccessMode accessMode, String bookmark)
    {
        this.parentSession = parentSession;
        this.v1Session = v1Session;
        this.accessMode = accessMode;

        this.v1Transaction = this.v1Session.beginTransaction(bookmark); // will be READ for a READ session, and WRITE for a WRITE session
    }

    @Override
    public void success()
    {
        v1Transaction.success();
    }

    @Override
    public void failure()
    {
        v1Transaction.failure();
    }

    @Override
    public boolean isOpen()
    {
        return v1Transaction.isOpen();
    }

    @Override
    public void close()
    {
        v1Transaction.close(); // this is the only action that changes the v1Session bookmark
        parentSession.setBookmark(v1Session.lastBookmark());
    }

    @Override
    public Outcome getOutcome()
    {
        return null;
        // this requires 1) access to impl object, 2) opening up private state info systematically,
        // and 3) returning null if not one of the Outcome states.
    }

    @Override
    public TransactionState getTransactionState()
    {
        return null;
        // this requires 1) access to impl object, 2) opening up private state info systematically
    }

    @Override
    public StatementResult run(String statementTemplate, Value parameters)
    {
        return v1Transaction.run(statementTemplate, parameters);
    }

    @Override
    public StatementResult run(String statementTemplate, Map<String, Object> statementParameters)
    {
        return v1Transaction.run(statementTemplate, statementParameters);
    }

    @Override
    public StatementResult run(String statementTemplate, Record statementParameters)
    {
        return v1Transaction.run(statementTemplate, statementParameters);
    }

    @Override
    public StatementResult run(String statementTemplate)
    {
        return v1Transaction.run(statementTemplate);
    }

    @Override
    public StatementResult run(Statement statement)
    {
        return v1Transaction.run(statement);
    }

    @Override
    public TypeSystem typeSystem()
    {
        return v1Transaction.typeSystem();
    }
}
