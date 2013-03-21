package org.neo4j.graphdatabases.queries.helpers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryUnionExecutionResult implements Iterable<Map<String, Object>>
{
    private final List<String> queries;
    private final ExecutionEngineWrapper executionEngine;
    private final Map<String, Object> params;

    public QueryUnionExecutionResult( List<String> queries,
                                      ExecutionEngineWrapper executionEngine,
                                      Map<String, Object> params )
    {
        this.queries = queries;
        this.executionEngine = executionEngine;
        this.params = params;
    }

    @Override
    public Iterator<Map<String, Object>> iterator()
    {
        return new ExecutionResultsIterator( queries, executionEngine, params );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(  );
        for ( String query : queries )
        {
            builder.append(  executionEngine.execute( query, params ).dumpToString());
        }
        return builder.toString();
    }
}
