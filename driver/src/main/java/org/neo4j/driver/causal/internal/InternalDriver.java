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
        return session(null, AccessMode.READ_WRITE, Consistency.CAUSAL, ToleranceForReplicationDelay.HIGH); // should we default to causal consistency within a session?
    }

    @Override
    public Session session(AccessMode accessMode)
    {
        return session(null, accessMode, Consistency.CAUSAL, ToleranceForReplicationDelay.HIGH);
    }

    @Override
    public Session session(AccessMode accessMode, Consistency consistencyLevel)
    {
        return session(null, accessMode, consistencyLevel, ToleranceForReplicationDelay.HIGH);
    }

    @Override
    public Session session(AccessMode accessMode, Consistency consistencyLevel, ToleranceForReplicationDelay toleranceForReplicationDelay)
    {
        return session(null, accessMode, consistencyLevel, toleranceForReplicationDelay);
    }

    @Override
    public Session session(String bookmark)
    {
        return session(bookmark, AccessMode.READ_WRITE, Consistency.CAUSAL, ToleranceForReplicationDelay.HIGH);
    }

    @Override
    public Session session(String bookmark, AccessMode accessMode)
    {
        return session(bookmark, accessMode, Consistency.CAUSAL, ToleranceForReplicationDelay.HIGH);
    }

    @Override
    public Session session(String bookmark, AccessMode accessMode, Consistency consistencyLevel)
    {
        return session(null, accessMode, consistencyLevel, ToleranceForReplicationDelay.HIGH);
    }

    @Override
    public Session session(String bookmark, AccessMode accessMode, Consistency consistencyLevel, ToleranceForReplicationDelay toleranceForReplicationDelay)
    {
        switch (accessMode)
        {
            case WRITE:
                return new ReadWriteSession(v1Driver, bookmark, accessMode, consistencyLevel, toleranceForReplicationDelay);
            case READ:
            default:
                return new ReadSession(v1Driver, bookmark, accessMode, consistencyLevel, toleranceForReplicationDelay);
        }
    }

    @Override
    public void close()
    {
        v1Driver.close();
    }
}
