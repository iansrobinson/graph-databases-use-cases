package org.neo4j.graphdatabases.performance_tests.testing;

import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;
import org.neo4j.graphdb.GraphDatabaseService;

public class DefaultExecutionEngineWrapper implements ExecutionEngineWrapper
{
    private final ExecutionEngine executionEngine;

    public DefaultExecutionEngineWrapper( GraphDatabaseService db )
    {
        this.executionEngine = new ExecutionEngine( db );
    }

    @Override
    public ExecutionResult execute( String query, Map<String, Object> params )
    {
        return executionEngine.execute( query, params );
    }

    @Override
    public ExecutionResult execute( String query, Map<String, Object> params, int index )
    {
        return execute( query, params );
    }
}
