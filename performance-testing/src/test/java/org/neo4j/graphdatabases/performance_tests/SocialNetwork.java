package org.neo4j.graphdatabases.performance_tests;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.*;
import org.junit.rules.TestName;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.SocialNetworkConfig;
import org.neo4j.graphdatabases.performance_tests.testing.DefaultExecutionEngineWrapper;
import org.neo4j.graphdatabases.performance_tests.testing.MultipleTestRuns;
import org.neo4j.graphdatabases.performance_tests.testing.ParamsGenerator;
import org.neo4j.graphdatabases.performance_tests.testing.ResultFormatter;
import org.neo4j.graphdatabases.performance_tests.testing.ResultsContainSameElementsUnordered;
import org.neo4j.graphdatabases.performance_tests.testing.SingleTest;
import org.neo4j.graphdatabases.performance_tests.testing.SysOutWriter;
import org.neo4j.graphdatabases.queries.SocialNetworkQueries;
import org.neo4j.graphdatabases.queries.helpers.DbUtils;
import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.graphdatabases.performance_tests.testing.DoNothingWithTestResults.doNothing;
import static org.neo4j.graphdatabases.performance_tests.testing.PrintTestResults.printResults;
import static org.neo4j.graphdatabases.performance_tests.testing.TakeXTestResults.take;
import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.probabilities.ProbabilityDistribution.flatDistribution;

public class SocialNetwork
{
    public static final int NUMBER_OF_RESULTS = 5;
    public static final int NUMBER_OF_TEST_RUNS = 20;

    private GraphDatabaseService db;
    private SocialNetworkQueries queries;
    private MultipleTestRuns multipleTestRuns;
    private Random random;
    private TestOutputWriter writer = SysOutWriter.INSTANCE;

    @Rule
    public TestName name = new TestName();

    @Before
    public void init()
    {
        db = DbUtils.existingDB( SocialNetworkConfig.STORE_DIR );

        queries = new SocialNetworkQueries( db, new DefaultExecutionEngineWrapper( db ) );
        multipleTestRuns = new MultipleTestRuns( NUMBER_OF_TEST_RUNS, writer );

        random = new Random();
    }

    @After
    public void teardown()
    {
        db.shutdown();
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
        try ( Transaction tx = db.beginTx() )
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
            tx.success();
        }
    }

    @Test
    public void friendOfAFriendWithInterestTraversalFramework() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
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
            tx.success();
        }
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
        try ( Transaction tx = db.beginTx() )
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
            tx.success();
        }
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

                List<Integer> topicIds = flatDistribution().generateList( 5, minMax( 1,
                        SocialNetworkConfig.NUMBER_TOPICS ) );

                Map<String, String> params = new HashMap<String, String>();
                for ( String key : keys )
                {
                    if ( key.equals( "user" ) )
                    {
                        params.put( "user", String.format( "User-%s",
                                random.nextInt( SocialNetworkConfig.NUMBER_USERS ) + 1 ) );
                    }
                    if ( key.equals( "topic1" ) )
                    {
                        params.put( "topic1", String.format( "Topic-%s", topicIds.get( 0 ) ) );
                    }
                    if ( key.equals( "topic2" ) )
                    {
                        params.put( "topic2", String.format( "Topic-%s", topicIds.get( 1 ) ) );
                    }
                    if ( key.equals( "topic3" ) )
                    {
                        params.put( "topic3", String.format( "Topic-%s", topicIds.get( 2 ) ) );
                    }
                    if ( key.equals( "topic4" ) )
                    {
                        params.put( "topic4", String.format( "Topic-%s", topicIds.get( 3 ) ) );
                    }
                    if ( key.equals( "topic5" ) )
                    {
                        params.put( "topic5", String.format( "Topic-%s", topicIds.get( 4 ) ) );
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
        params.put( "person", "User-309491" );
        params.put( "colleague", "User-558252" );
        params.put( "topic",  "Topic-78" );


        String cypher = "MATCH (person:User {name:{person}}), (colleague:User {name:{colleague}}), " +
                "(topic:Topic {name:{topic}})\n" +
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
                "User-309491",
                "Topic-78", NUMBER_OF_RESULTS );


        for ( Node node : nodes )
        {

        }
    }
}
