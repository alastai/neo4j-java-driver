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
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import java.util.Map;

import static org.neo4j.driver.causal.UnitOfWorkRetryParameters.DEFAULT_UNIT_OF_WORK_RETRY_PARAMETERS;

public class GraphDatabase
{
    public static Driver driver(String uri, AuthToken authToken)
    {
        // TODO discriminate between bolt:// and bolt+routing://?

        return driver(uri, authToken, DEFAULT_UNIT_OF_WORK_RETRY_PARAMETERS);
    }

    public static Driver driver(String uri, AuthToken authToken, UnitOfWorkRetryParameters unitOfWorkRetryParameters)
    {
        // TODO discriminate between bolt:// and bolt+routing://?

        return new InternalDriver(org.neo4j.driver.v1.GraphDatabase.driver(uri, authToken), unitOfWorkRetryParameters);
    }

    public static Driver driver(AuthToken authToken, String... uris) // just alternatives, cycle for the lazy app
    {
        return driver(authToken, uris);
    }

    public static Driver driver(Iterable<String> uris, AuthToken authToken) // just alternatives, cycle for the lazy app
    {
        // this bears thinking about -- should these be network addresses (bolt+routing:// assumed)?
        // that would prevent mixed URI schemes, which is one source of confusion/error for the client

        for (String uri: uris)
        {
            try
            {
                return driver(uri, authToken);
            }
            catch (ServiceUnavailableException serviceUnavailableException)
            {
                // try the next URI
            }
            catch (Exception exception) // something unexpected: let it fly
            {
                throw exception;
            }
        }
        throw new ServiceUnavailableException("None of the supplied routing bootstrap URIs worked");
    }

    public static Map<String, Driver> superClusterDrivers(AuthToken authToken, Iterable<String> partitionNames, String ... uris)
    {
        return superClusterDrivers(authToken, partitionNames, uris);
    }

    public static Map<String, Driver> superClusterDrivers(Iterable<String> uris, Iterable<String> partionNames, AuthToken authToken) // music of the future
    {
        // TODO return new InternalDriver(org.neo4j.driver.v1.GraphDatabase.driver(uri, authToken));
        return null;
    }
}
