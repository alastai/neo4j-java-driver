package org.neo4j.driver.causal;

import org.neo4j.driver.v1.exceptions.Neo4jException;

public class UnknownTransactionOutcomeException extends Neo4jException
{
    public UnknownTransactionOutcomeException(String message)
    {
        super(message);
    }

    public UnknownTransactionOutcomeException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
