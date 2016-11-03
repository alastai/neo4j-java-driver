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
