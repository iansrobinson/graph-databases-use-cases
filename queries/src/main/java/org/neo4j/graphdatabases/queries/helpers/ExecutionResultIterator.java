package org.neo4j.graphdatabases.queries.helpers;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.neo4j.cypher.javacompat.ExecutionResult;

public class ExecutionResultIterator
{
    public static ExecutionResultIterator newQueryIterators( List<String> queries,
                                                             ExecutionEngineWrapper executionEngine,
                                                             Map<String, Object> params )
    {
        Queue<String> queue = new LinkedList<String>( queries );
        String query = queue.poll();
        ExecutionResult executionResult = executionEngine.execute( query, params, 1 );
        return new ExecutionResultIterator( executionResult, queue, executionEngine, params, 2 );
    }

    private final ExecutionResult currentResult;
    private final Queue<String> queries;
    private final ExecutionEngineWrapper executionEngine;
    private final Map<String, Object> params;
    private final int queryIndex;

    private ExecutionResultIterator( ExecutionResult currentResult,
                                     Queue<String> queries,
                                     ExecutionEngineWrapper executionEngine,
                                     Map<String, Object> params,
                                     int queryIndex )
    {
        this.currentResult = currentResult;
        this.queries = queries;
        this.executionEngine = executionEngine;
        this.params = params;
        this.queryIndex = queryIndex;
    }

    public Iterator<Map<String, Object>> iterator()
    {
        return currentResult.iterator();
    }

    public boolean hasNextIterator()
    {
        return !queries.isEmpty();
    }

    public ExecutionResultIterator getNextIterator()
    {
        String query = queries.poll();
        ExecutionResult executionResult = executionEngine.execute( query, params, queryIndex );
        return new ExecutionResultIterator( executionResult, queries, executionEngine, params, queryIndex + 1 );
    }
}
