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

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.StatementRunner;
import org.neo4j.driver.v1.util.Resource;

public interface Transaction extends Resource, StatementRunner
{
    AccessMode accessMode();
    void success();
    void failure();
    @Override void close();
    Outcome getOutcome(); // only meaningful after close() ... which is all wrong
                          // this poses the need for a commit() and a rollback()
    Outcome commit();
    Outcome rollback();
    TransactionState getTransactionState();
}
