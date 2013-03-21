package org.neo4j.graphdatabases.performance_tests.testing;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;

public class ResultsContainSameElementsUnordered implements SingleTestRunResultHandler
{
    public static SingleTestRunResultHandlerFactory newFactory(final String... cleanResultRegExes)
    {
        return new SingleTestRunResultHandlerFactory()
        {
            @Override
            public SingleTestRunResultHandler createSingleTestRunResultHandler()
            {
                return new ResultsContainSameElementsUnordered(cleanResultRegExes);
            }
        };
    }

    private final List<String> cleanResultRegExes;
    private final Map<String, List<String>> results;
    private boolean newlineWritten = false;

    public ResultsContainSameElementsUnordered( String... cleanResultRegExes )
    {
        this.cleanResultRegExes = asList( cleanResultRegExes );
        results = new TreeMap<String, List<String>>();
    }

    @Override
    public void handle( String queryType, String formattedResult )
    {
        if ( !results.containsKey( queryType ) )
        {
            results.put( queryType, new ArrayList<String>() );
        }

        for ( String cleanResultRegEx : cleanResultRegExes )
        {
            formattedResult = formattedResult.replaceAll( cleanResultRegEx, "" );
        }

        results.get( queryType ).add( formattedResult );
    }

    @Override
    public void summarize( TestOutputWriter writer )
    {
        List<String> queryTypes = new ArrayList<String>( results.keySet() );
        for ( int i = 0; i < queryTypes.size() - 1; i++ )
        {
            String queryType1 = queryTypes.get( i );
            for ( int j = i + 1; j < queryTypes.size(); j++ )
            {
                String queryType2 = queryTypes.get( j );
                compareCollections( queryType1, results.get( queryType1 ), queryType2, results.get( queryType2 ),
                        writer );
            }
        }
    }

    private void compareCollections( String queryType1, Collection<String> results1, String queryType2,
                                     Collection<String> results2, TestOutputWriter writer )
    {
        compare( queryType1, results1, queryType2, results2, writer );
        compare( queryType2, results2, queryType1, results1, writer );
    }

    private void compare( String queryType1, Collection<String> results1, String queryType2,
                          Collection<String> results2, TestOutputWriter writer )
    {
        Collection<String> copyOfResults1 = new HashSet<String>( results1 );
        copyOfResults1.removeAll( results2 );
        if ( !copyOfResults1.isEmpty() )
        {
            if (!newlineWritten)
            {
                writer.writeln( "" );
                newlineWritten = true;
            }

            writer.writeln( String.format( "   %s v. %s: %s contains additional elements: %s", queryType1, queryType2,
                    queryType1, copyOfResults1 ) );

        }
    }


}
