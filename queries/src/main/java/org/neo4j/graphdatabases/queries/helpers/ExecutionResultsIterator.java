package org.neo4j.graphdatabases.queries.helpers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ExecutionResultsIterator implements Iterator<Map<String, Object>>
{
    private ExecutionResultIterator executionResultIterator;

    public ExecutionResultsIterator( List<String> queries,
                                     ExecutionEngineWrapper executionEngine,
                                     Map<String, Object> params )
    {
        executionResultIterator = ExecutionResultIterator.newQueryIterators( queries, executionEngine, params );
    }

    @Override
    public boolean hasNext()
    {
        if ( executionResultIterator.iterator().hasNext() )
        {
            return true;
        }

        if ( !executionResultIterator.hasNextIterator() )
        {
            return false;
        }

        executionResultIterator = executionResultIterator.getNextIterator();

        return hasNext();
    }

    @Override
    public Map<String, Object> next()
    {
        if ( hasNext() )
        {
            return executionResultIterator.iterator().next();
        }

        throw new NoSuchElementException();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
