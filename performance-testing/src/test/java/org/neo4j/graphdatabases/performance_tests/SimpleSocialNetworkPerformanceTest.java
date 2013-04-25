package org.neo4j.graphdatabases.performance_tests;

import static org.neo4j.graphdatabases.performance_tests.testing.PrintTestResults.printResults;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.SimpleSocialNetwork;
import org.neo4j.graphdatabases.performance_tests.testing.MultipleTestRuns;
import org.neo4j.graphdatabases.performance_tests.testing.ParamsGenerator;
import org.neo4j.graphdatabases.performance_tests.testing.SingleTest;
import org.neo4j.graphdatabases.performance_tests.testing.SysOutWriter;
import org.neo4j.graphdatabases.queries.SimpleSocialNetworkQueries;
import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdatabases.queries.traversals.FriendOfAFriendDepth4;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

@Ignore
public class SimpleSocialNetworkPerformanceTest
{
    private static GraphDatabaseService db;
    private static SimpleSocialNetworkQueries queries;
    private static MultipleTestRuns multipleTestRuns;
    private static Random random;
    private static TestOutputWriter writer = SysOutWriter.INSTANCE;

    public static final int NUMBER_OF_TEST_RUNS = 10;

    @BeforeClass
    public static void init()
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( "dump_configuration", "true" );
        params.put( "cache_type", "gcr" );

        db = new GraphDatabaseFactory()
                            .newEmbeddedDatabaseBuilder( SimpleSocialNetwork.STORE_DIR )
                            .setConfig( params )
                            .newGraphDatabase();
        queries = new SimpleSocialNetworkQueries( db );
        multipleTestRuns = new MultipleTestRuns( NUMBER_OF_TEST_RUNS, writer );

        random = new Random();
    }

    @AfterClass
    public static void teardown()
    {
        db.shutdown();
    }

    @Test
    public void foafToDepthFour() throws Exception
    {
        // when
        multipleTestRuns.execute( "Foaf to depth 4", createParams(), printResults( 1, writer ), new SingleTest()
        {
            @Override
            public String queryType()
            {
                return "Cypher";
            }

            @Override
            public ExecutionResult execute( Map<String, String> params )
            {
                return queries.pathBetweenTwoFriends( params.get( "first-user" ), params.get( "second-user" ) );
            }
        } );

    }

    @Test
    public void friendOfAFriendToDepth4() throws Exception
    {
        // when
        multipleTestRuns.execute( "Friend of a friend to depth 4", createParams(), printResults( 100, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.friendOfAFriendToDepth4( params.get( "first-user" ) );
                    }
                } );

    }

    @Test
    public void onlyFriendsAtDepth4UsingTraversalFramework() throws Exception
    {
        final FriendOfAFriendDepth4 traversal = new FriendOfAFriendDepth4( db );

        // when
        multipleTestRuns.execute( "Only friends at depth 4 using Traversal Framework", createParams(),
                printResults( 300000, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Traversal Framework (custom class)";
                    }

                    @Override
                    public Iterable<Node> execute( Map<String, String> params )
                    {
                        return traversal.getFriends( params.get( "first-user" ) );
                    }
                } );
    }

    private ParamsGenerator createParams()
    {
        return new ParamsGenerator()
        {
            @Override
            public Map<String, String> generateParams()
            {
                Map<String, String> params = new HashMap<String, String>();
                params.put( "first-user", String.format( "user-%s", random.nextInt( SimpleSocialNetwork.NUMBER_USERS
                ) + 1 ) );
                params.put( "second-user", String.format( "user-%s", random.nextInt( SimpleSocialNetwork.NUMBER_USERS
                ) + 1 ) );
                return params;
            }
        };
    }
}
