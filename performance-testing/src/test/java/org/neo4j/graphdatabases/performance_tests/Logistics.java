package org.neo4j.graphdatabases.performance_tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.LogisticsConfig;
import org.neo4j.graphdatabases.performance_tests.testing.DefaultExecutionEngineWrapper;
import org.neo4j.graphdatabases.performance_tests.testing.MultipleTestRuns;
import org.neo4j.graphdatabases.performance_tests.testing.ParamsGenerator;
import org.neo4j.graphdatabases.performance_tests.testing.SingleTest;
import org.neo4j.graphdatabases.performance_tests.testing.SysOutWriter;
import org.neo4j.graphdatabases.queries.LogisticsQueries;
import org.neo4j.graphdatabases.queries.helpers.DbUtils;
import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.Arrays.asList;

import static org.neo4j.graphdatabases.performance_tests.testing.PrintTestResults.printResults;

public class Logistics
{
    public static final int NUMBER_OF_TEST_RUNS = 20;
    private static final int NUMBER_OF_RESULTS = 15;

    private static GraphDatabaseService db;
    private static LogisticsQueries queries;
    private static MultipleTestRuns multipleTestRuns;
    private static Random random;
    private static TestOutputWriter writer = SysOutWriter.INSTANCE;

    @BeforeClass
    public static void init()
    {
        db = DbUtils.existingDB( LogisticsConfig.STORE_DIR );

        queries = new LogisticsQueries( db, new DefaultExecutionEngineWrapper( db ) );
        multipleTestRuns = new MultipleTestRuns( NUMBER_OF_TEST_RUNS, writer );

        random = new Random();

    }

    @AfterClass
    public static void teardown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }


    @Test
    public void queryTest() throws Exception
    {
        List<Integer> customIds = asList( 6053, 210, 6, 56, 8, 87, 2, 95, 9, 256, 7476, 545580 );
        List<Integer> cypherIds = asList( 6053, 210, 6, 192, 9, 256, 7476, 545580 );
        Interval interval = Interval.parse( "2012-10-17T00:00:00.000+01:00/2012-10-18T00:00:00.000+01:00" );

        getPath( customIds, interval );
        getPath( cypherIds, interval );

    }

    private void getPath( List<Integer> ids, Interval interval )
    {
        StringBuilder sbWhere = new StringBuilder();
        StringBuilder sbMatch = new StringBuilder();

        for ( int i = 0; i < ids.size(); i++ )
        {
            sbWhere.append("id(n");
            sbWhere.append(i);
            sbWhere.append(")=");
            sbWhere.append(ids.get(i));
            if ( i < (ids.size() - 1) )
            {
                sbWhere.append(" AND ");
            }
            sbWhere.append(" ");

            sbMatch.append( "n" );
            sbMatch.append( i );
            if ( i < (ids.size() - 1) )
            {
                sbMatch.append( "--" );
            }
        }

        String q = String.format(
                "MATCH p = %s %n" +
                "WHERE %s AND ALL(r in relationships(p) where r.start_date <= %s and r.end_date >= %s) %n" +
                "RETURN REDUCE(weight=0, r in relationships(p) | weight+r.cost) AS score, p",
                sbMatch.toString(),
                sbWhere.toString(),
                interval.getStartMillis(),
                interval.getEndMillis());
        writer.writeln(q);


        ExecutionResult result = new ExecutionEngine( db ).execute( q );
        writer.writeln(result.toString());
    }

    @After
    public void flush()
    {
        writer.flush();
    }

    @Test
    public void shortestRoute() throws Exception
    {
        TestRunParams testRunParams = new TestRunParams( db, writer );

        // when
        multipleTestRuns.execute( "Shortest route through the parcel system",
                testRunParams.createParams(),
                printResults( NUMBER_OF_RESULTS, writer ),

                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public Object execute( Map<String, String> params )
                    {
                        return queries.findShortestPathWithCypherReduce(
                                params.get( "start" ),
                                params.get( "end" ),
                                Interval.parse( params.get( "interval" ) ) );

                    }
                },
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "ParcelRouteCalculator";
                    }

                    @Override
                    public Object execute( Map<String, String> params )
                    {
                        return queries.findShortestPathWithParcelRouteCalculator(
                                params.get( "start" ),
                                params.get( "end" ),
                                Interval.parse( params.get( "interval" ) ) );

                    }
                }
        );
    }

    @Test
    public void testSingleCypherQuery() throws Exception
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( "start", "delivery-segment-181622" );
        params.put( "end", "delivery-segment-611694" );
        params.put( "interval", "2012-10-17T00:00:00.000-07:00/2012-10-18T00:00:00.000-07:00" );


        Interval interval = Interval.parse( params.get( "interval" ) );
        System.out.println( "Start: " + interval.getStartMillis() );
        System.out.println( "End: " + interval.getEndMillis() );
        ExecutionResult result = queries.findShortestPathWithCypherReduce(
                params.get( "start" ),
                params.get( "end" ),
                interval );

        writer.writeln( result.toString() );
    }

    private class TestRunParams
    {
        private int deliveryAreaCount;
        private int deliverySegmentCount;

        public TestRunParams( GraphDatabaseService db, TestOutputWriter writer )
        {
            GlobalGraphOperations ops = GlobalGraphOperations.at(db);
            try ( Transaction tx = db.beginTx())
            {
                deliveryAreaCount = IteratorUtil.count(ops.getAllNodesWithLabel(DynamicLabel.label("DeliveryArea")));
                deliverySegmentCount = IteratorUtil.count(ops.getAllNodesWithLabel(DynamicLabel.label("DeliverySegment")));

                writer.writeln( "deliveryAreaCount " + deliveryAreaCount );
                writer.writeln( "deliverySegmentCount " + deliverySegmentCount );
                tx.success();
            }
        }

        public ParamsGenerator createParams()
        {
            return new ParamsGenerator()
            {
                @Override
                public final Map<String, String> generateParams()
                {
                    Map<String, String> params = new HashMap<String, String>();
                    if ( random.nextInt( 2 ) < 1 )
                    {
                        params.put( "start",
                                String.format( "DeliverySegment-%s", random.nextInt( deliverySegmentCount ) + 1 ) );
                    }
                    else
                    {
                        params.put( "start",
                                String.format( "DeliveryArea-%s", random.nextInt( deliveryAreaCount ) + 1 ) );
                    }
                    params.put( "end",
                            String.format( "DeliverySegment-%s", random.nextInt( deliverySegmentCount ) + 1 ) );
                    DateTime startDtm = LogisticsConfig.START_DATE.plusDays( random.nextInt( 6 ) );
                    params.put( "interval", new Interval( startDtm, startDtm.plusDays( 1 ) ).toString() );
                    return params;
                }
            };
        }

    }

}
