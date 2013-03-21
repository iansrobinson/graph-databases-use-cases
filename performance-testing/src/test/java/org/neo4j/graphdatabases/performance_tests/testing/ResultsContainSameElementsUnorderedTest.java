package org.neo4j.graphdatabases.performance_tests.testing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;

public class ResultsContainSameElementsUnorderedTest
{
    @Test
    public void shouldReportDiscrepanciesBetweenTwoQueries() throws Exception
    {
        // given
        ResultsContainSameElementsUnordered handler = new ResultsContainSameElementsUnordered(  );

        // when
        handler.handle( "queryType1", "a" );
        handler.handle( "queryType2", "C" );
        handler.handle( "queryType1", "b" );
        handler.handle( "queryType2", "b" );
        handler.handle( "queryType1", "c" );
        handler.handle( "queryType2", "a" );

        MyTestOutputWriter writer = new MyTestOutputWriter();
        handler.summarize( writer );

        // then
        String expected = "\n" +
                          "   queryType1 v. queryType2: queryType1 contains additional elements: [c]\n" +
                          "   queryType2 v. queryType1: queryType2 contains additional elements: [C]\n";

        assertEquals( expected, writer.toString());

    }

    @Test
    public void shouldReportDiscrepanciesBetweenThreeQueries() throws Exception
    {
        // given
        ResultsContainSameElementsUnordered handler = new ResultsContainSameElementsUnordered(  );

        // when
        handler.handle( "queryType1", "a" );
        handler.handle( "queryType2", "b" );
        handler.handle( "queryType3", "c" );

        MyTestOutputWriter writer = new MyTestOutputWriter();
        handler.summarize( writer );

        // then
        String expected = "\n" +
                          "   queryType1 v. queryType2: queryType1 contains additional elements: [a]\n" +
                          "   queryType2 v. queryType1: queryType2 contains additional elements: [b]\n" +
                          "   queryType1 v. queryType3: queryType1 contains additional elements: [a]\n" +
                          "   queryType3 v. queryType1: queryType3 contains additional elements: [c]\n" +
                          "   queryType2 v. queryType3: queryType2 contains additional elements: [b]\n" +
                          "   queryType3 v. queryType2: queryType3 contains additional elements: [c]\n";

        assertEquals( expected, writer.toString());
    }

    private static class MyTestOutputWriter implements TestOutputWriter
    {
        private final StringBuilder builder = new StringBuilder(  );

        @Override
        public void begin()
        {
        }

        @Override
        public void write( String value )
        {
            builder.append( value );
        }

        @Override
        public void writeln( String value )
        {
            builder.append( value );
            builder.append( "\n" );
        }

        @Override
        public void flush()
        {
        }

        @Override
        public void end()
        {
        }

        @Override
        public String toString()
        {
            return builder.toString();
        }
    }
}
