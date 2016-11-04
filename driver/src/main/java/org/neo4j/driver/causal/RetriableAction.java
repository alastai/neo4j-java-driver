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

public class RetriableAction<RESULT, EXCEPTION extends Throwable>
{
    @FunctionalInterface
    public interface Action<RESULT, EXCEPTION extends Throwable>
    {
        RESULT work() throws EXCEPTION;
    }

    public static <RESULT, EXCEPTION extends Throwable> Outcome<RESULT, EXCEPTION> attemptWork(Action<RESULT, EXCEPTION> action)
    {
        return new RetriableAction<RESULT, EXCEPTION>().tryWork(action);
    }

    public Outcome<RESULT, EXCEPTION> tryWork(Action<RESULT, EXCEPTION> action)
    {
        try
        {
            return Outcome.succeeded((action.work()));
        }
        catch (Throwable exception)
        {
            try
            {
                return Outcome.failed((EXCEPTION) exception);
            }
            catch (ClassCastException classCastException)
            {
                throw new RuntimeException("Unexpected runtime exception", exception);
            }
        }
    }

    public static class Outcome<RESULT, EXCEPTION extends Throwable>
    {
        public static <RESULT, EXCEPTION extends Throwable>
        Outcome<RESULT, EXCEPTION> succeeded(RESULT result)
        {
            return new Outcome<>(result);
        }

        public static <RESULT, EXCEPTION extends Throwable>
        Outcome<RESULT, EXCEPTION> failed(EXCEPTION exception)
        {
            return new Outcome<>(exception);
        }

        private final RESULT result;
        private final EXCEPTION exception;

        private Outcome(EXCEPTION sessionExpiredException)
        {
            this.result = null;
            this.exception = sessionExpiredException;
        }

        private Outcome(RESULT result)
        {
            this.result = result;
            this.exception = null;
        }

        public RESULT result()
        {
            return result;
        }

        public EXCEPTION exception()
        {
            return exception;
        }

        public boolean succeeded()
        {
            return (result != null);
        }

        public boolean failed()
        {
            return (exception != null);
        }
    }
}
