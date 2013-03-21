package org.neo4j.graphdatabases.performance_tests.testing;


import java.util.Iterator;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;

public class TakeXTestResults implements TestResultsHandler
{
    public static TestResultsHandler take( int quantity )
    {
        return new TakeXTestResults( quantity );
    }

    private final int quantity;

    public TakeXTestResults( int quantity )
    {
        this.quantity = quantity;
    }

    @Override
    public void handle( String queryType, Object results, SingleTestRunResultHandler singleTestRunResultHandler )
    {
        int i = 0;
        if ( Iterable.class.isAssignableFrom( results.getClass() ) )
        {
            Iterator iterator = ((Iterable) results).iterator();
            while ( iterator.hasNext() && ++i < quantity )
            {
                iterator.next();
            }
        }
    }

    @Override
    public void writeTo( TestOutputWriter writer )
    {
        writer.writeln( String.format( "NUMBER_OF_RESULTS: %s", quantity ) );
    }

}
