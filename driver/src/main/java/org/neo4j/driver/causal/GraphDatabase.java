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
package org.neo4j.driver.causal;

import org.neo4j.driver.causal.internal.InternalDriver;
import org.neo4j.driver.v1.AuthToken;

public class GraphDatabase
{
    // TODO establish retry defaults to be used for session creation attempts
    // and to be inherited as defaults for begin transaction and run attempts

    public static Driver driver(String uri, AuthToken authToken)
    {
        // TODO discriminate between bolt:// and bolt+routing://?

        return new InternalDriver(org.neo4j.driver.v1.GraphDatabase.driver(uri, authToken));
    }

    public static Driver driver(AuthToken authToken, String... uri) // just alternatives, cycle for the lazy app
    {
        // TODO return new InternalDriver(org.neo4j.driver.v1.GraphDatabase.driver(uri, authToken));
        return null;
    }

    public static Driver driver(Iterable<String> uri, AuthToken authToken) // just alternatives, cycle for the lazy app
    {
        // TODO return new InternalDriver(org.neo4j.driver.v1.GraphDatabase.driver(uri, authToken));
        return null;
    }

    public static Driver superClusterDriver(AuthToken authToken, String... uri)
    {
        // TODO return new InternalDriver(org.neo4j.driver.v1.GraphDatabase.driver(uri, authToken));
        return null;
    }

    public static Driver superClusterDriver(Iterable<String> uri, AuthToken authToken) // music of the future
    {
        // TODO return new InternalDriver(org.neo4j.driver.v1.GraphDatabase.driver(uri, authToken));
        return null;
    }

}
