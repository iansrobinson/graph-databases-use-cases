package org.neo4j.graphdatabases.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.GraphDatabaseService;

public class SimpleSocialNetworkQueriesTest
{
    private static GraphDatabaseService db;
    private static SimpleSocialNetworkQueries queries;

    @BeforeClass
    public static void init()
    {
        db = createDatabase();
        queries = new SimpleSocialNetworkQueries( db );
    }

    @AfterClass
    public static void shutdown()
    {
        db.shutdown();
    }

    @Test
    public void shouldReturnShortestPathBetweenTwoFriends() throws Exception
    {
        // when
        ExecutionResult results = queries.pathBetweenTwoFriends( "Ben", "Mike" );

        // then
        assertTrue( results.iterator().hasNext() );
        assertEquals( 4, results.iterator().next().get( "depth" ) );
    }

    @Test
    public void friendOfAFriendToDepth4() throws Exception
    {
        // when
        ExecutionResult results = queries.friendOfAFriendToDepth4( "Ben" );

        // then
        assertTrue( results.iterator().hasNext() );
        assertEquals( "Mike", results.iterator().next().get( "name" ) );
        assertFalse( results.iterator().hasNext() );
    }

    @Test
    public void shouldReturnNoResultsWhenThereIsNotAPathBetweenTwoFriends() throws Exception
    {
        // when
        ExecutionResult results = queries.pathBetweenTwoFriends( "Ben", "Arnold" );

        // then
        assertFalse( results.iterator().hasNext() );
    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE\n" +
                "(ben:User {name:'Ben'}),\n" +
                "(arnold:User {name:'Arnold'}),\n" +
                "(charlie:User {name:'Charlie'}),\n" +
                "(gordon:User {name:'Gordon'}),\n" +
                "(lucy:User {name:'Lucy'}),\n" +
                "(emily:User {name:'Emily'}),\n" +
                "(sarah:User {name:'Sarah'}),\n" +
                "(kate:User {name:'Kate'}),\n" +
                "(mike:User {name:'Mike'}),\n" +
                "(paula:User {name:'Paula'}),\n" +
                "ben-[:FRIEND]->charlie,\n" +
                "charlie-[:FRIEND]->lucy,\n" +
                "lucy-[:FRIEND]->sarah,\n" +
                "sarah-[:FRIEND]->mike,\n" +
                "arnold-[:FRIEND]->gordon,\n" +
                "gordon-[:FRIEND]->emily,\n" +
                "emily-[:FRIEND]->kate,\n" +
                "kate-[:FRIEND]->paula";

        return createFromCypher(
                "Simple Social Network",
                cypher,
                IndexParam.indexParam( "User", "name" ) );
    }
}
