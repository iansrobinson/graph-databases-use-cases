package org.neo4j.graphdatabases.performance_tests;

import static org.neo4j.graphdatabases.performance_tests.testing.DoNothingWithTestResults.doNothing;
import static org.neo4j.graphdatabases.performance_tests.testing.PrintTestResults.printResults;
import static org.neo4j.graphdatabases.performance_tests.testing.TakeXTestResults.take;
import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.probabilities.ProbabilityDistribution.flatDistribution;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.AccessControlNoAttributes;
import org.neo4j.graphdatabases.SocialNetwork;
import org.neo4j.graphdatabases.performance_tests.testing.DefaultExecutionEngineWrapper;
import org.neo4j.graphdatabases.performance_tests.testing.MultipleTestRuns;
import org.neo4j.graphdatabases.performance_tests.testing.ParamsGenerator;
import org.neo4j.graphdatabases.performance_tests.testing.ResultFormatter;
import org.neo4j.graphdatabases.performance_tests.testing.ResultsContainSameElementsUnordered;
import org.neo4j.graphdatabases.performance_tests.testing.SingleTest;
import org.neo4j.graphdatabases.performance_tests.testing.SysOutWriter;
import org.neo4j.graphdatabases.queries.SocialNetworkQueries;
import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;

@Ignore
public class SocialNetworkPerformanceTest
{
    public static final int NUMBER_OF_RESULTS = 5;
    public static final int NUMBER_OF_TEST_RUNS = 100;

    private static GraphDatabaseService db;
    private static SocialNetworkQueries queries;
    private static MultipleTestRuns multipleTestRuns;
    private static Random random;
    private static TestOutputWriter writer = SysOutWriter.INSTANCE;

    @Rule
    public static TestName name = new TestName();

    @BeforeClass
    public static void init()
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( "dump_configuration", "true" );
        params.put( "cache_type", "gcr" );

        try
        {
            db = new GraphDatabaseFactory()
                                .newEmbeddedDatabaseBuilder( SocialNetwork.STORE_DIR )
                                .setConfig( params )
                                .newGraphDatabase();
        }
        catch ( Exception e )
        {
            writer.writeln( "Error in init(): " + e.getMessage() );
            e.printStackTrace();
        }

        queries = new SocialNetworkQueries( db, new DefaultExecutionEngineWrapper( db ) );
        multipleTestRuns = new MultipleTestRuns( NUMBER_OF_TEST_RUNS, writer );

        random = new Random();

        //warmCache( db, writer);
//
//        GraphStatistics.create( db, SocialNetwork.TITLE )
//                .describeTo( new AsciiDocFormatter( SysOutLog.INSTANCE ) );

    }

    @AfterClass
    public static void teardown()
    {
        db.shutdown();
    }

    @After
    public void flush()
    {
        writer.flush();
    }

    @Test
    public void sharedInterestsSameCompany() throws Exception
    {
        // when
        multipleTestRuns.execute( name.getMethodName(), createParams( "user" ),
                take( NUMBER_OF_RESULTS ), new SingleTest()
        {
            @Override
            public String queryType()
            {
                return "Cypher";
            }

            @Override
            public Object execute( Map<String, String> params )
            {
                return queries.sharedInterestsSameCompany( params.get( "user" ) );
            }
        } );

    }

    @Test
    public void sharedInterestsAllCompanies() throws Exception
    {
        // when
        multipleTestRuns.execute( name.getMethodName(), createParams( "user" ),
                take( NUMBER_OF_RESULTS ), new SingleTest()
        {
            @Override
            public String queryType()
            {
                return "Cypher";
            }

            @Override
            public Object execute( Map<String, String> params )
            {
                return queries.sharedInterestsAllCompanies( params.get( "user" ), NUMBER_OF_RESULTS );
            }
        } );
    }

    @Test
    public void sharedInterestsAlsoInterestedInTopic() throws Exception
    {
        // when
        multipleTestRuns.execute( name.getMethodName(),
                createParams( "user", "topic1" ), take( NUMBER_OF_RESULTS ), new SingleTest()
        {
            @Override
            public String queryType()
            {
                return "Cypher";
            }

            @Override
            public Object execute( Map<String, String> params )
            {
                return queries.sharedInterestsAlsoInterestedInTopic( params.get( "user" ), params.get( "topic1" ) );
            }
        } );
    }

    @Test
    public void friendOfAFriendWithInterest() throws Exception
    {
        // when
        multipleTestRuns.execute(
                name.getMethodName(), createParams( "user", "topic1" ), take( NUMBER_OF_RESULTS ),
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
                        return queries.friendOfAFriendWithInterest( params.get( "user" ),
                                params.get( "topic1" ), NUMBER_OF_RESULTS );
                    }
                } );
    }

    @Test
    public void friendOfAFriendWithInterestTraversalFramework() throws Exception
    {
        // when
        multipleTestRuns.execute( name.getMethodName(), createParams( "user", "topic1" ),
                doNothing(), new SingleTest()
        {
            @Override
            public String queryType()
            {
                return "Traversal Framework";
            }

            @Override
            public Object execute( Map<String, String> params )
            {
                return queries.friendOfAFriendWithInterestTraversalFramework(
                        params.get( "user" ), params.get( "topic1" ), NUMBER_OF_RESULTS );
            }
        } );
    }


//    @Test
//    public void shouldFindColleagueOfAColleagueOfAColleagueEtcWithAParticularInterestUsingGremlin() throws Exception
//    {
//        // when
//        multipleTestRuns.execute( "Colleagues of colleagues interested in particular topic (gremlin)",
//                createParams(), printResults( 2 ), new SingleTestRun()
//        {
//            @Override
//            public QueryType queryType()
//            {
//                return QueryType.Gremlin;
//            }
//
//            @Override
//            public Object execute( Map<String, String> params )
//            {
//                try
//                {
//                    return queries.friendOfAFriendWithParticularInterestGremlin( params.get( "user" ),
//                            params.get( "topic" ) );
//                }
//                catch ( ScriptException e )
//                {
//                    throw new RuntimeException( e );
//                }
//            }
//        } );
//    }

    @Test
    public void queryBakeoff() throws Exception
    {
        // when
        multipleTestRuns.execute( name.getMethodName(), createParams( "user", "topic1" ),
                printResults( NUMBER_OF_RESULTS, resultFormatter(), writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Traversal Framework";
                    }

                    @Override
                    public Object execute( Map<String, String> params )
                    {
                        return queries.friendOfAFriendWithInterestTraversalFramework(
                                params.get( "user" ), params.get( "topic1" ), NUMBER_OF_RESULTS );
                    }
                }
//                , new SingleTestRun()
//                {
//                    @Override
//                    public QueryType queryType()
//                    {
//                        return QueryType.Gremlin;
//                    }
//
//                    @Override
//                    public Object execute( Map<String, String> params )
//                    {
//                        try
//                        {
//                            return queries.friendOfAFriendWithParticularInterestGremlin( params.get( "user" ),
//                                    params.get( "topic" ) );
//                        }
//                        catch ( ScriptException e )
//                        {
//                            throw new RuntimeException( e );
//                        }
//                    }
//                }
                , new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public Object execute( Map<String, String> params )
                    {
                        return queries.friendOfAFriendWithInterest(
                                params.get( "user" ), params.get( "topic1" ), NUMBER_OF_RESULTS );
                    }
                }
                , new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher2";
                    }

                    @Override
                    public Object execute( Map<String, String> params )
                    {
                        return queries.friendWorkedWithFriendWithInterests(
                                params.get( "user" ), NUMBER_OF_RESULTS,
                                params.get( "topic1" ) );
                    }
                }
        );
    }

    @Test
    public void friendWorkedWithFriendWithInterests() throws Exception
    {
        // when
        multipleTestRuns.execute(
                name.getMethodName(),
                createParams( "user", "topic1", "topic2", "topic3" ),
                take( NUMBER_OF_RESULTS ), new SingleTest()
        {
            @Override
            public String queryType()
            {
                return "Cypher";
            }

            @Override
            public ExecutionResult execute( Map<String, String> params )
            {
                return queries.friendWorkedWithFriendWithInterests( params.get( "user" ),
                        NUMBER_OF_RESULTS, params.get( "topic1" ), params.get( "topic2" ), params.get( "topic3" ) );
            }
        } );
    }

    @Test
    public void friendOfAFriendWithMultipleInterestsBakeoff() throws Exception
    {
        // when
        multipleTestRuns.execute(
                name.getMethodName(),
                createParams( "user", "topic1", "topic2", "topic3", "topic4", "topic5" ),
                ResultsContainSameElementsUnordered.newFactory(),
                printResults( NUMBER_OF_RESULTS, resultFormatter(), writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "friendOfAFriendWithMultipleInterest";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.friendOfAFriendWithMultipleInterest( params.get( "user" ),
                                NUMBER_OF_RESULTS,
                                params.get( "topic1" ),
                                params.get( "topic2" ),
                                params.get( "topic3" ),
                                params.get( "topic4" ),
                                params.get( "topic5" ) );
                    }
                },
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "friendWorkedWithFriendWithInterests";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.friendWorkedWithFriendWithInterests( params.get( "user" ),
                                NUMBER_OF_RESULTS,
                                params.get( "topic1" ),
                                params.get( "topic2" ),
                                params.get( "topic3" ),
                                params.get( "topic4" ),
                                params.get( "topic5" ) );
                    }
                }
        );
    }

    private ParamsGenerator createParams( final String... keys )
    {
        return new ParamsGenerator()
        {
            @Override
            public final Map<String, String> generateParams()
            {

                List<Integer> topicIds = flatDistribution().generateList( 5, minMax( 1, SocialNetwork.NUMBER_TOPICS ) );

                Map<String, String> params = new HashMap<String, String>();
                for ( String key : keys )
                {
                    if ( key.equals( "user" ) )
                    {
                        params.put( "user", String.format( "user-%s",
                                random.nextInt( SocialNetwork.NUMBER_USERS ) + 1 ) );
                    }
                    if ( key.equals( "topic1" ) )
                    {
                        params.put( "topic1", String.format( "topic-%s", topicIds.get( 0 ) ) );
                    }
                    if ( key.equals( "topic2" ) )
                    {
                        params.put( "topic2", String.format( "topic-%s", topicIds.get( 1 ) ) );
                    }
                    if ( key.equals( "topic3" ) )
                    {
                        params.put( "topic3", String.format( "topic-%s", topicIds.get( 2 ) ) );
                    }
                    if ( key.equals( "topic4" ) )
                    {
                        params.put( "topic4", String.format( "topic-%s", topicIds.get( 3 ) ) );
                    }
                    if ( key.equals( "topic5" ) )
                    {
                        params.put( "topic5", String.format( "topic-%s", topicIds.get( 4 ) ) );
                    }
                }

                return params;
            }
        };
    }

    private ResultFormatter resultFormatter()
    {
        return new ResultFormatter()
        {
            @Override
            public String format( Object result )
            {
                if ( Node.class.isAssignableFrom( result.getClass() ) )
                {
                    return ((Node) result).getProperty( "name" ).toString();
                }
                else
                {
                    return result.toString();
                }
            }
        };
    }

    @Test
    @Ignore
    public void testSingleCypherQuery() throws Exception
    {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "person", "name:" + "user-309491" );
        params.put( "colleague", "name:" + "user-558252" );
        params.put( "topic", "name:" + "topic-78" );


        String cypher = "START person=node:user({person}), colleague=node:user({colleague}), " +
                "topic=node:topic({topic})\n" +
                "MATCH p = person-[:WORKED_ON*2..4]-colleague-[:INTERESTED_IN]->topic\n" +
                "RETURN p, LENGTH(p) AS pathLength ORDER BY pathLength ASC";


        ExecutionResult result = new ExecutionEngine( db ).execute( cypher, params );


        writer.writeln( result.toString() );
    }

    @Test
    @Ignore
    public void testTraversal() throws Exception
    {
        Collection<Node> nodes = queries.friendOfAFriendWithInterestTraversalFramework(
                "user-309491",
                "topic-78", NUMBER_OF_RESULTS );


        for ( Node node : nodes )
        {

        }
    }
}
