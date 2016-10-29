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

public interface Driver extends AutoCloseable
{
    boolean isEncrypted();
    Session session();                                                      // default to ACCESS_MODE.READ_WRITE
                                                                            // default to Consistency.CAUSAL
                                                                            // default to ToleranceForReplicationDelay.HIGH

    Session session(AccessMode accessMode);                                 // default to Consistency.CAUSAL
                                                                            // default to ToleranceForReplicationDelay.HIGH

    Session session(AccessMode accessMode,
                    Consistency consistencyLevel);                         // default to ToleranceForReplicationDelay.HIGH

    Session session(AccessMode accessMode,
                    Consistency consistencyLevel,
                    ToleranceForReplicationDelay toleranceForReplicationDelay);

    Session session(String bookmark);                                       // default to ACCESS_MODE.READ_WRITE
                                                                            // default to Consistency.CAUSAL
                                                                            // default to ToleranceForReplicationDelay.HIGH

    Session session(String bookmark, AccessMode accessMode);                // default to Consistency.CAUSAL
                                                                            // default to ToleranceForReplicationDelay.HIGH

    Session session(String bookmark,
                    AccessMode accessMode,
                    Consistency consistencyLevel);                         // default to ToleranceForReplicationDelay.HIGH

    Session session(String bookmark,
                    AccessMode accessMode,
                    Consistency consistencyLevel,
                    ToleranceForReplicationDelay toleranceForReplicationDelay);
}
