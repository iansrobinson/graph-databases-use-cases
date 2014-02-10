package org.neo4j.graphdatabases.queries.helpers;

import java.io.PrintWriter;
import java.util.Map;

import org.junit.rules.TestName;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.AsciiDocGenerator;

public class PrintingExecutionEngineWrapper implements ExecutionEngineWrapper
{
    private final ExecutionEngine executionEngine;
    private final String useCase;
    private final TestName testName;

    public PrintingExecutionEngineWrapper( GraphDatabaseService db, String useCase, TestName testName )
    {
        this.useCase = useCase;
        this.executionEngine = new ExecutionEngine( db );
        this.testName = testName;
    }

    @Override
    public ExecutionResult execute( String query, Map<String, Object> params )
    {
        return execute( query, params, 1 );
    }

    @Override
    public ExecutionResult execute( String query, Map<String, Object> params, int index )
    {
        printQuery( query, index );
        ExecutionResult returnResult = executionEngine.execute( query, params );
        // For CREATE queries, this may not return the same results
        ExecutionResult printResult = executionEngine.execute( query, params );
        printResult( printResult, index );
        return returnResult;
    }

    private void printResult( org.neo4j.cypher.javacompat.ExecutionResult results, int index )
    {
        String output = "[queryresult]\n----\n" + results.dumpToString()
                + "\n----\n";
        printFile( useCase + "-" + testName.getMethodName() + "-result-" + index, output );
    }

    private void printQuery( String query, int index )
    {
        String output = "----\n" + query + "\n----\n";
        printFile( useCase + "-" + testName.getMethodName() + "-query-" + index, output );
    }

    private static void printFile( String fileName, String contents )
    {
        try (PrintWriter writer = AsciiDocGenerator.getPrintWriter("../examples", fileName)) {
            writer.println(contents);
        }
    }
}
