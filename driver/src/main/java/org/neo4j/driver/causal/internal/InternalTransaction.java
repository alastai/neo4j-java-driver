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
import org.neo4j.driver.causal.RetriableAction;
import org.neo4j.driver.causal.Transaction;
import org.neo4j.driver.causal.TransactionState;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.types.TypeSystem;

import java.util.Map;

public class InternalTransaction implements Transaction
{
    // TODO configure these values

    final static int DEFAULT_ATTEMPTS_TO_RUN = 3;
    final static int DEFAULT_ATTEMPTS_TO_BEGIN_TRANSACTION = 3;

    private final BookmarkingSession parentSession;
    private final AccessMode accessMode;
    private final org.neo4j.driver.v1.Transaction v1Transaction;
    private org.neo4j.driver.causal.Outcome outcome;

    public AccessMode getAccessMode()
    {
        return this.accessMode;
    }

    public InternalTransaction(BookmarkingSession parentSession, AccessMode accessMode)
    {
        this.parentSession = parentSession;
        this.accessMode = accessMode;

        this.v1Transaction = resilientBeginTransaction(parentSession, null); // will be READ for a READ session, and WRITE for a WRITE session
    }

    public InternalTransaction(BookmarkingSession parentSession, AccessMode accessMode, String bookmark)
    {
        this.parentSession = parentSession;
        this.accessMode = accessMode;

        this.v1Transaction = resilientBeginTransaction(parentSession, bookmark); // will be READ for a READ session, and WRITE for a WRITE session
    }

    private org.neo4j.driver.v1.Transaction resilientBeginTransaction(BookmarkingSession parentSession, String bookmark)
    {
        RetriableAction.Outcome<org.neo4j.driver.v1.Transaction, Neo4jException> beginTransactionOutcome = null;

        int attemptsToBeginTransaction = DEFAULT_ATTEMPTS_TO_BEGIN_TRANSACTION;
        int attempted = 0;
        while (attempted < attemptsToBeginTransaction) // the number of attempts intended
        {
            beginTransactionOutcome = RetriableAction.attemptWork(() ->
            {
                if (null == bookmark)
                {
                    return parentSession.v1Session().beginTransaction();
                }
                else
                {
                    return parentSession.v1Session().beginTransaction(bookmark);
                }
            });

            if (beginTransactionOutcome.succeeded())
            {
                return beginTransactionOutcome.result();
            }
            try
            {
                parentSession.refreshV1Session(); // this is worth doing: sessions can be established on still-live or new-role servers
            }
            catch (ServiceUnavailableException serviceUnavailableException)
            {
                // in a sense this is pointless, belabouring the point that this exception can fly out
                // however, if we imagine logging that this happened here, then it would have a function
                // and it is also (heavy-handedly) self-documenting -- you can't miss that this can happen

                throw serviceUnavailableException;
            }
            attempted++;
        }
        throw beginTransactionOutcome.exception();
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
        parentSession.setBookmark(parentSession.v1Session().lastBookmark()); // do this unconditionally, it will be a no-op if not bookmarking (null on null)
    }

    @Override
    public org.neo4j.driver.causal.Outcome getOutcome()
    {
        return null;

        // TODO
        // this requires 1) access to impl object, 2) opening up private state info systematically,
        // and 3) returning null if not one of the Outcome states.
    }

    @Override
    public TransactionState getTransactionState()
    {
        return null;

        // TODO
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
        return resilientRun(statementTemplate, statementParameters);
    }

/* playing with streams and reflection:


    private StatementResult resilientRunT(String statementTemplate, Object statementParameters) throws Exception
    {
        Class statementParametersClass = statementParameters.getClass();
        Class v1TransactionClass = v1Transaction.getClass();
        List<Method> methods = Arrays.asList(v1TransactionClass.getMethods());

        Method methodToInvoke = methods.stream()
                .filter(method -> method.getName().equals("run"))
                .filter(method ->
                {
                    Stream parameters = Stream.of(method.getParameterTypes());
                    return parameters
                            .anyMatch(parameter -> (parameter.getClass().getName().equals(statementParametersClass.getName())));
                })
                .findFirst()
                .get();

        RetriableAction.Outcome<StatementResult, Neo4jException> runOutcome = null;

        int attemptsToRun = DEFAULT_ATTEMPTS_TO_RUN; // it's not clear that we need to loop like this. If the second attempt fails,
                                                     // isn't the server dead?
        int attempted = 0;

        while (attempted < attemptsToRun) // the number of attempts intended
        {
            runOutcome = RetriableAction.attemptWork(() ->
            {
                try
                {
                    return methodToInvoke.invoke(v1Transaction, statementTemplate, statementParametersClass.cast(statementParameters));
                }
                catch (IllegalAccessException e)
                {
                    e.printStackTrace();
                }
                catch (InvocationTargetException e)
                {
                    e.printStackTrace();
                }
            });

            if (runOutcome.succeeded())
            {
                return runOutcome.result();
            }
            try
            {
                parentSession.refreshV1Session(); // this is worth doing: sessions can be established on still-live or new-role servers
            }
            catch (ServiceUnavailableException serviceUnavailableException)
            {
                // in a sense this is pointless, belabouring the point that this exception can fly out
                // however, if we imagine logging that this happened here, then it would have a function
                // and it is also (heavy-handedly) self-documenting -- you can't miss that this can happen

                throw serviceUnavailableException;
            }
            attempted++;
        }
        throw runOutcome.exception();
    }
*/

    private StatementResult resilientRun(String statementTemplate, Map<String, Object> statementParameters)
    {
        RetriableAction.Outcome<StatementResult, Neo4jException> runOutcome = null;

        int attemptsToRun = DEFAULT_ATTEMPTS_TO_RUN; // it's not clear that we need to loop like this. If the second attempt fails,
        // isn't the server dead?
        int attempted = 0;

        while (attempted < attemptsToRun) // the number of attempts intended
        {
            runOutcome = RetriableAction.attemptWork(() ->
            {
                return v1Transaction.run(statementTemplate, statementParameters);
            });

            if (runOutcome.succeeded())
            {
                return runOutcome.result();
            }
            try
            {
                parentSession.refreshV1Session(); // this is worth doing: sessions can be established on still-live or new-role servers
            }
            catch (ServiceUnavailableException serviceUnavailableException)
            {
                // in a sense this is pointless, belabouring the point that this exception can fly out
                // however, if we imagine logging that this happened here, then it would have a function
                // and it is also (heavy-handedly) self-documenting -- you can't miss that this can happen

                throw serviceUnavailableException;
            }
            attempted++;
        }
        throw runOutcome.exception();
    }

    @Override
    public StatementResult run(String statementTemplate, Record statementParameters)
    {
        return resilientRun(statementTemplate, statementParameters);
    }

    private StatementResult resilientRun(String statementTemplate, Record statementParameters)
    {
        RetriableAction.Outcome<StatementResult, Neo4jException> runOutcome = null;

        int attemptsToRun = DEFAULT_ATTEMPTS_TO_RUN; // it's not clear that we need to loop like this. If the second attempt fails,
        // isn't the server dead?
        int attempted = 0;

        while (attempted < attemptsToRun) // the number of attempts intended
        {
            runOutcome = RetriableAction.attemptWork(() ->
            {
                return v1Transaction.run(statementTemplate, statementParameters);
            });

            if (runOutcome.succeeded())
            {
                return runOutcome.result();
            }
            try
            {
                parentSession.refreshV1Session(); // this is worth doing: sessions can be established on still-live or new-role servers
            }
            catch (ServiceUnavailableException serviceUnavailableException)
            {
                // in a sense this is pointless, belabouring the point that this exception can fly out
                // however, if we imagine logging that this happened here, then it would have a function
                // and it is also (heavy-handedly) self-documenting -- you can't miss that this can happen

                throw serviceUnavailableException;
            }
            attempted++;
        }
        throw runOutcome.exception();
    }

    @Override
    public StatementResult run(String statementTemplate)
    {
        return resilientRun(statementTemplate);
    }

    private StatementResult resilientRun(String statementTemplate)
    {
        RetriableAction.Outcome<StatementResult, Neo4jException> runOutcome = null;

        int attemptsToRun = DEFAULT_ATTEMPTS_TO_RUN; // it's not clear that we need to loop like this. If the second attempt fails,
        // isn't the server dead?
        int attempted = 0;

        while (attempted < attemptsToRun) // the number of attempts intended
        {
            runOutcome = RetriableAction.attemptWork(() ->
            {
                return v1Transaction.run(statementTemplate);
            });

            if (runOutcome.succeeded())
            {
                return runOutcome.result();
            }
            try
            {
                parentSession.refreshV1Session(); // this is worth doing: sessions can be established on still-live or new-role servers
            }
            catch (ServiceUnavailableException serviceUnavailableException)
            {
                // in a sense this is pointless, belabouring the point that this exception can fly out
                // however, if we imagine logging that this happened here, then it would have a function
                // and it is also (heavy-handedly) self-documenting -- you can't miss that this can happen

                throw serviceUnavailableException;
            }
            attempted++;
        }
        throw runOutcome.exception();
    }

    @Override
    public StatementResult run(Statement statement)
    {
        return resilientRun(statement);
    }

    private StatementResult resilientRun(Statement statement)
    {
        RetriableAction.Outcome<StatementResult, Neo4jException> runOutcome = null;

        int attemptsToRun = DEFAULT_ATTEMPTS_TO_RUN; // it's not clear that we need to loop like this. If the second attempt fails,
        // isn't the server dead?
        int attempted = 0;

        while (attempted < attemptsToRun) // the number of attempts intended
        {
            runOutcome = RetriableAction.attemptWork(() ->
            {
                return v1Transaction.run(statement);
            });

            if (runOutcome.succeeded())
            {
                return runOutcome.result();
            }
            try
            {
                parentSession.refreshV1Session(); // this is worth doing: sessions can be established on still-live or new-role servers
            }
            catch (ServiceUnavailableException serviceUnavailableException)
            {
                // in a sense this is pointless, belabouring the point that this exception can fly out
                // however, if we imagine logging that this happened here, then it would have a function
                // and it is also (heavy-handedly) self-documenting -- you can't miss that this can happen

                throw serviceUnavailableException;
            }
            attempted++;
        }
        throw runOutcome.exception();
    }

    @Override
    public TypeSystem typeSystem()
    {
        return v1Transaction.typeSystem();
    }
}
