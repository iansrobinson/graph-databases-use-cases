package org.neo4j.graphdatabases.queries.helpers;

import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.Map;

public class QueryUnionExecutionEngine
{
    private final ExecutionEngineWrapper executionEngine;

    public QueryUnionExecutionEngine( ExecutionEngineWrapper executionEngine )
    {
        this.executionEngine = executionEngine;
    }

    public QueryUnionExecutionResult execute( final Map<String, Object> params, final String... queries )
    {
        if ( queries.length == 0 )
        {
            throw new IllegalArgumentException( "Must supply one or more queries." );
        }

        return new QueryUnionExecutionResult( asList( queries ), executionEngine, params );
    }

    public Iterable<Map<String, Object>> execute( final String... queries )
    {
        return execute( new HashMap<String, Object>(), queries );
    }
}
