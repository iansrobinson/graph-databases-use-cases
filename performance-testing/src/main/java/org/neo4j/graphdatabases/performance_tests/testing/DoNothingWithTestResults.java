package org.neo4j.graphdatabases.performance_tests.testing;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;

public class DoNothingWithTestResults implements TestResultsHandler
{
    public static TestResultsHandler doNothing()
    {
        return new DoNothingWithTestResults();
    }

    @Override
    public void handle( String queryType, Object results, SingleTestRunResultHandler singleTestRunResultHandler )
    {
        //Do nothing
    }

    @Override
    public void writeTo( TestOutputWriter writer )
    {
        // Do nothing
    }

}
