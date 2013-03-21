package org.neo4j.graphdatabases.performance_tests.testing;

import java.util.Iterator;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;

public class PrintTestResults implements TestResultsHandler
{
    private static ResultFormatter DEFAULT_FORMATTER = new ResultFormatter()
    {
        @Override
        public String format( Object result )
        {
            return result.toString();
        }
    };

    public static TestResultsHandler printResults( int numberOfResults, ResultFormatter resultFormatter,
                                                     TestOutputWriter writer )
    {
        return new PrintTestResults( numberOfResults, resultFormatter, writer );
    }

    public static TestResultsHandler printResults( int numberOfResults, TestOutputWriter writer )
    {
        return new PrintTestResults( numberOfResults, DEFAULT_FORMATTER, writer );
    }

    private final int numberOfResults;
    private final ResultFormatter resultFormatter;
    private final TestOutputWriter writer;

    private PrintTestResults( int numberOfResults, ResultFormatter resultFormatter, TestOutputWriter writer )
    {
        this.numberOfResults = numberOfResults;
        this.resultFormatter = resultFormatter;
        this.writer = writer;
    }

    @Override
    public void handle( String queryType, Object results, SingleTestRunResultHandler singleTestRunResultHandler )
    {
        if ( Iterable.class.isAssignableFrom( results.getClass() ) )
        {
            Iterator iterator = ((Iterable) results).iterator();
            int count = 0;

            if ( !iterator.hasNext() )
            {
                writer.write( " {EMPTY}" );
            }
            else
            {
                while ( iterator.hasNext() && count < numberOfResults )
                {
                    String formattedResult = resultFormatter.format( iterator.next() );
                    singleTestRunResultHandler.handle( queryType, formattedResult );
                    writer.writeln( String.format( "     [%s]", formattedResult ) );
                    count++;
                }
                writer.writeln("");
                writer.writeln(String.format( "     Total: %s", count ) );
                writer.writeln("");
            }
        }
        else
        {
            String formattedResult = resultFormatter.format( results );
            singleTestRunResultHandler.handle( queryType, formattedResult );
            writer.writeln( formattedResult );
        }
    }

    @Override
    public void writeTo( TestOutputWriter writer )
    {
        writer.writeln( String.format( "NUMBER_OF_RESULTS: %s", numberOfResults ) );
    }

}
