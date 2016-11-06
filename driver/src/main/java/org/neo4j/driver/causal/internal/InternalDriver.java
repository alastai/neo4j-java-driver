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
import org.neo4j.driver.causal.Driver;
import org.neo4j.driver.causal.Session;
import org.neo4j.driver.causal.ToleranceForReplicationDelay;

public class InternalDriver implements Driver
{
    private final org.neo4j.driver.v1.Driver v1Driver;

    public InternalDriver(org.neo4j.driver.v1.Driver v1Driver)
    {
        this.v1Driver = v1Driver;
    }

    @Override
    public boolean isEncrypted()
    {
        return v1Driver.isEncrypted();
    }

    @Override
    public Session session()
    {
        return session(AccessMode.WRITE, Consistency.CAUSAL, ToleranceForReplicationDelay.LOW, null); // should we default to causal consistency within a session?
    }

    @Override
    public Session session(AccessMode accessMode)
    {
        return session(accessMode, Consistency.CAUSAL, ToleranceForReplicationDelay.LOW, null);
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
    public Session session(AccessMode accessMode, Consistency consistency)
    {
        return session(accessMode, consistency, ToleranceForReplicationDelay.LOW, null);
    }

    @Override
    public Session session(AccessMode accessMode, ToleranceForReplicationDelay toleranceForReplicationDelay)
    {
        return session(accessMode, Consistency.CAUSAL, toleranceForReplicationDelay, null);
    }

    @Override
    public Session session(AccessMode accessMode, Consistency consistency, ToleranceForReplicationDelay toleranceForReplicationDelay)
    {
        return session(accessMode, consistency, toleranceForReplicationDelay, null);
    }

    @Override
    public Session session(String bookmark)
    {
        return session(AccessMode.WRITE, Consistency.CAUSAL, ToleranceForReplicationDelay.LOW, bookmark);
    }

    @Override
    public Session session(AccessMode accessMode, String bookmark)
    {
        return session(accessMode, Consistency.CAUSAL, ToleranceForReplicationDelay.LOW, bookmark);
    }

    @Override
    public Session session(AccessMode accessMode, ToleranceForReplicationDelay toleranceForReplicationDelay, String bookmark)
    {
        return session(accessMode, Consistency.CAUSAL, toleranceForReplicationDelay, bookmark);
    }

    private Session session(AccessMode accessMode, Consistency consistency, ToleranceForReplicationDelay toleranceForReplicationDelay, String bookmark)
    {
        switch (accessMode)
        {
            case WRITE:
                return new WriteSession(v1Driver, consistency, toleranceForReplicationDelay, bookmark);
            case READ:
            default:
                return new ReadSession(v1Driver, consistency, toleranceForReplicationDelay, bookmark);
        }
    }

    @Override
    public void close()
    {
        v1Driver.close();
    }
}
