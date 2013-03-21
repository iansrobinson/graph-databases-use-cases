package org.neo4j.graphdatabases.queries.helpers;

import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;


public interface ExecutionEngineWrapper
{
    ExecutionResult execute(String query, Map<String, Object> params);
    ExecutionResult execute(String query, Map<String, Object> params, int index);
}
