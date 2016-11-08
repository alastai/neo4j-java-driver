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

import org.neo4j.driver.causal.Consistency;
import org.neo4j.driver.causal.Driver;
import org.neo4j.driver.causal.Session;
import org.neo4j.driver.causal.ToleranceForReplicationDelay;
import org.neo4j.driver.causal.UnitOfWorkRetryParameters;

public class InternalDriver implements Driver
{
    private final org.neo4j.driver.v1.Driver v1Driver;
    private final UnitOfWorkRetryParameters defaultUnitOfWorkRetryParameters;

    public InternalDriver(org.neo4j.driver.v1.Driver v1Driver, UnitOfWorkRetryParameters defaultUnitOfWorkRetryParameters)
    {
        this.v1Driver = v1Driver;
        this.defaultUnitOfWorkRetryParameters = defaultUnitOfWorkRetryParameters;
    }

    @Override
    public boolean isEncrypted()
    {
        return v1Driver.isEncrypted();
    }

    @Override
    public UnitOfWorkRetryParameters defaultRetryUnitOfWorkParameters()
    {
        return this.defaultUnitOfWorkRetryParameters;
    }

    @Override
    public Session session()
    {
        return session(Consistency.CAUSAL, ToleranceForReplicationDelay.LOW, null); // should we default to causal consistency within a session?
    }

    @Override
    public Session session(Consistency consistency)
    {
        return null;
    }

    @Override
    public Session session(ToleranceForReplicationDelay toleranceForReplicationDelay)
    {
        return null;
    }

    @Override
    public Session session(Consistency consistency, ToleranceForReplicationDelay toleranceForReplicationDelay)
    {
        return session(consistency, toleranceForReplicationDelay, null);
    }

    @Override
    public Session session(String bookmark)
    {
        return session(Consistency.CAUSAL, ToleranceForReplicationDelay.LOW, bookmark);
    }

    @Override
    public Session session(ToleranceForReplicationDelay toleranceForReplicationDelay, String bookmark)
    {
        return session(Consistency.CAUSAL, toleranceForReplicationDelay, bookmark);
    }

    private Session session(Consistency consistency, ToleranceForReplicationDelay toleranceForReplicationDelay, String bookmark)
    {
        return new InternalSession(v1Driver, consistency, toleranceForReplicationDelay, this.defaultUnitOfWorkRetryParameters, bookmark);
    }

    @Override
    public void close()
    {
        v1Driver.close();
    }
}
