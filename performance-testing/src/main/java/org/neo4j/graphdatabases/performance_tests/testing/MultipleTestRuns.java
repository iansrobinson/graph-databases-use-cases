package org.neo4j.graphdatabases.performance_tests.testing;

import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.probabilities.ProbabilityDistribution.flatDistribution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;

public class MultipleTestRuns
{
    private final int numberOfRuns;
    private final TestOutputWriter writer;

    public MultipleTestRuns( int numberOfRuns, TestOutputWriter writer )
    {
        this.numberOfRuns = numberOfRuns;
        this.writer = writer;
    }

    public void execute( String title,
                         ParamsGenerator paramsGenerator,
                         SingleTestRunResultHandlerFactory singleTestRunResultHandlerFactory,
                         TestResultsHandler testResultsHandler,
                         SingleTest... tests )
    {
        writer.writeln( title );
        testResultsHandler.writeTo( writer );

        Map<String, Long> totalTimes = new HashMap<String, Long>();
        for ( SingleTest singleTest : tests )
        {
            totalTimes.put( singleTest.queryType(), 0L );
        }

        for ( long i = 0; i < numberOfRuns; i++ )
        {
            SingleTestRunResultHandler singleTestRunResultHandler = singleTestRunResultHandlerFactory
                    .createSingleTestRunResultHandler();

            writer.writeln( String.format( "\nTest run %s of %s", i + 1, numberOfRuns ) );
            Map<String, String> params = paramsGenerator.generateParams();

            if ( !params.isEmpty() )
            {
                writer.write( "Params: " );
            }
            for ( String key : params.keySet() )
            {
                writer.write( String.format( "[%s: %s] ", key, params.get( key ) ) );
            }

            writer.writeln( "" );

            //Randomize the order in which tests are executed each run
            List<Integer> testIndexes = flatDistribution()
                    .generateList( tests.length, minMax( 0, tests.length - 1 ) );

            for ( Integer testIndex : testIndexes )
            {
                SingleTest singleTest = tests[testIndex];
                writer.writeln( String.format( "\n  %s", singleTest.queryType() ) );
                long startTime = System.nanoTime();
                Object lastResult = singleTest.execute( params );
                testResultsHandler.handle( singleTest.queryType(), lastResult, singleTestRunResultHandler );
                long endTime = System.nanoTime();
                long duration = endTime - startTime;
                writer.writeln( String.format( "  Duration (ms): %s", duration / 1000000 ) );
                Long currentTotalTime = totalTimes.get( singleTest.queryType() );
                totalTimes.put( singleTest.queryType(), currentTotalTime + duration );
            }

            singleTestRunResultHandler.summarize( writer );

        }

        writer.writeln( "\n======================================" );
        writer.writeln( title );
        writer.writeln( "Average times (ms)" );

        for ( SingleTest singleTest : tests )
        {
            long avgTime = totalTimes.get( singleTest.queryType() ) / numberOfRuns / 1000000;
            writer.writeln( String.format( "  %s: %s", singleTest.queryType(), avgTime ) );
        }
        writer.writeln( "======================================" );
    }

    public void execute( String title,
                         ParamsGenerator paramsGenerator,
                         TestResultsHandler testResultsHandler,
                         SingleTest... tests )
    {
        execute( title, paramsGenerator, new NullSingleTestRunResultHandlerFactory(), testResultsHandler, tests );
    }

    private static class NullSingleTestRunResultHandler implements SingleTestRunResultHandler
    {
        @Override
        public void handle( String queryType, String formattedResult )
        {
            // Do nothing
        }

        @Override
        public void summarize( TestOutputWriter writer )
        {
            // Do nothing
        }
    }

    private static class NullSingleTestRunResultHandlerFactory implements SingleTestRunResultHandlerFactory
    {
        private final  SingleTestRunResultHandler instance = new NullSingleTestRunResultHandler();

        @Override
        public SingleTestRunResultHandler createSingleTestRunResultHandler()
        {
            return instance;
        }
    }
}
