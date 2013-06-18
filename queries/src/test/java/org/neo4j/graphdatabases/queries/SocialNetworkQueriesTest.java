package org.neo4j.graphdatabases.queries;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.PrintingExecutionEngineWrapper;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class SocialNetworkQueriesTest
{
    @Rule
    public TestName name = new TestName();

    private GraphDatabaseService db;
    private SocialNetworkQueries queries;

    @Before
    public void init()
    {
        db = createDatabase();
        queries = new SocialNetworkQueries( db, new PrintingExecutionEngineWrapper( db, "social-network", name ) );
    }

    @After
    public void shutdown()
    {
        db.shutdown();
    }

    @Test
    public void sharedInterestsSameCompany() throws Exception
    {
        // when
        ExecutionResult results = queries.sharedInterestsSameCompany( "Sarah" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        Map<String, Object> result = iterator.next();
        assertEquals( "Ben", result.get( "name" ) );
        assertEquals( 2l, result.get( "score" ) );
        assertEquals( asList( "Graphs", "REST" ), result.get( "interests" ) );

        result = iterator.next();
        assertEquals( "Charlie", result.get( "name" ) );
        assertEquals( 1l, result.get( "score" ) );
        assertEquals( asList( "Graphs" ), result.get( "interests" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void sharedInterestsAllCompanies() throws Exception
    {
        // when
        ExecutionResult results = queries.sharedInterestsAllCompanies( "Sarah", 10 );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        Map<String, Object> result;

        result = iterator.next();
        assertEquals( "Arnold", result.get( "name" ) );
        assertEquals( "Startup, Ltd", result.get( "company" ) );
        assertEquals( 3l, result.get( "score" ) );
        assertEquals( asList( "Java", "Graphs", "REST" ), result.get( "interests" ) );

        result = iterator.next();
        assertEquals( "Ben", result.get( "name" ) );
        assertEquals( "Acme, Inc", result.get( "company" ) );
        assertEquals( 2l, result.get( "score" ) );
        assertEquals( asList( "Graphs", "REST" ), result.get( "interests" ) );

        result = iterator.next();
        assertEquals( "Gordon", result.get( "name" ) );
        assertEquals( "Startup, Ltd", result.get( "company" ) );
        assertEquals( 1l, result.get( "score" ) );
        assertEquals( asList( "Graphs" ), result.get( "interests" ) );

        result = iterator.next();
        assertEquals( "Charlie", result.get( "name" ) );
        assertEquals( "Acme, Inc", result.get( "company" ) );
        assertEquals( 1l, result.get( "score" ) );
        assertEquals( asList( "Graphs" ), result.get( "interests" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void sharedInterestsAlsoInterestedInTopic() throws Exception
    {
        // when
        ExecutionResult results = queries.sharedInterestsAlsoInterestedInTopic( "Ben", "Travel" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        Map<String, Object> result = iterator.next();
        assertEquals( "Arnold", result.get( "name" ) );
        assertEquals( asList( "Graphs", "Java", "REST", "Travel" ), result.get( "topics" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void friendOfAFriendWithInterest() throws Exception
    {
        // when
        ExecutionResult results = queries.friendOfAFriendWithInterest( "Sarah", "Java", 3 );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        Map<String, Object> result = iterator.next();
        assertEquals( "Arnold", result.get( "name" ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void friendOfAFriendWithInterestTraversalFramework() throws Exception
    {
        // when
        Collection<Node> results = queries.friendOfAFriendWithInterestTraversalFramework( "Arnold", "Art", 5 );

        // then
        Iterator<Node> iterator = results.iterator();
        assertEquals( "Emily", iterator.next().getProperty( "name" ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void friendWorkedWithFriendWithInterests() throws Exception
    {
        // when
        createAllWorkedWithRelationships();

        ExecutionResult results = queries.friendWorkedWithFriendWithInterests( "Arnold", 5, "Art", "Design" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        Map<String, Object> result = iterator.next();
        assertEquals( "Emily", result.get( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void friendOfAFriendWithMultipleInterest() throws Exception
    {
        // when
        ExecutionResult results = queries.friendOfAFriendWithMultipleInterest( "Arnold", 5, "Art", "Design" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        Map<String, Object> result = iterator.next();
        assertEquals( "Emily", result.get( "name" ) );
        assertEquals( 2L, result.get( "score" ) );
        assertEquals( 2L, result.get( "distance" ) );
        assertEquals( asList( "Design", "Art" ), result.get( "interests" ) );


        assertFalse( iterator.hasNext() );
    }

    @Test
    public void friendOfAFriendWithMultipleInterestShouldOrderByScore() throws Exception
    {
        // when
        ExecutionResult results = queries.friendOfAFriendWithMultipleInterest( "Sarah", 5, "Java", "Travel", "Medicine" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        Map<String, Object> result = iterator.next();
        assertEquals( "Arnold", result.get( "name" ) );
        assertEquals( 2L, result.get( "score" ) );
        assertEquals( 2L, result.get( "distance" ) );
        assertEquals( asList( "Travel", "Java" ), result.get( "interests" ) );

        result = iterator.next();
        assertEquals( "Charlie", result.get( "name" ) );
        assertEquals( 1L, result.get( "score" ) );
        assertEquals( 1L, result.get( "distance" ) );
        assertEquals( asList( "Medicine" ), result.get( "interests" ) );


        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldCreateNewWorkedWithRelationships() throws Exception
    {
        // given
        String cypher = "START sarah=node:user(name='Sarah'),\n" +
                "ben=node:user(name='Ben')\n" +
                "CREATE sarah-[:WORKED_WITH]->ben";

        createFromCypher( db, "Enriched Social Network", cypher );

        // when
        ExecutionResult results = queries.createWorkedWithRelationships( "Sarah" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        Map<String, Object> result = iterator.next();
        assertEquals( "Emily", result.get( "endName" ) );

        result = iterator.next();
        assertEquals( "Charlie", result.get( "endName" ) );

        result = iterator.next();
        assertEquals( "Kate", result.get( "endName" ) );

        assertFalse( iterator.hasNext() );
    }

    private void createAllWorkedWithRelationships()
    {
        ExecutionResult allUsers = queries.getAllUsers();
        Iterator<Map<String, Object>> iterator = allUsers.iterator();
        while (iterator.hasNext())
        {
            queries.createWorkedWithRelationships( iterator.next().get( "name" ).toString() );
        }
    }



//    @Test
//    public void runGremlinQuery() throws Exception
//    {
//        // given
//        GremlinPipeline result = queries.friendOfAFriendWithParticularInterestGremlin( "Arnold", "Art" );
//
//        assertTrue( Iterable.class.isAssignableFrom( result.getClass() ) );
//
//        // then
//        Iterator iterator = result.iterator();
//
//        Object next = iterator.next();
//        assertEquals( "Emily", next );
//        assertFalse( iterator.hasNext() );
//    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE\n" +
                "(ben {name:'Ben', _label:'user'}),\n" +
                "(arnold {name:'Arnold', _label:'user'}),\n" +
                "(charlie {name:'Charlie', _label:'user'}),\n" +
                "(gordon {name:'Gordon', _label:'user'}),\n" +
                "(lucy {name:'Lucy', _label:'user'}),\n" +
                "(emily {name:'Emily', _label:'user'}),\n" +
                "(sarah {name:'Sarah', _label:'user'}),\n" +
                "(kate {name:'Kate', _label:'user'}),\n" +
                "(acme {name:'Acme, Inc', _label:'company'}),\n" +
                "(startup {name:'Startup, Ltd', _label:'company'}),\n" +
                "(graphs {name:'Graphs', _label:'topic'}),\n" +
                "(rest {name:'REST', _label:'topic'}),\n" +
                "(art {name:'Art', _label:'topic'}),\n" +
                "(design {name:'Design', _label:'topic'}),\n" +
                "(medicine {name:'Medicine', _label:'topic'}),\n" +
                "(drama {name:'Drama', _label:'topic'}),\n" +
                "(java {name:'Java', _label:'topic'}),\n" +
                "(music {name:'Music', _label:'topic'}),\n" +
                "(cars {name:'Cars', _label:'topic'}),\n" +
                "(travel {name:'Travel', _label:'topic'}),\n" +
                "(phoenix {name:'Phoenix', _label:'project'}),\n" +
                "(quantumLeap {name:'Quantum Leap', _label:'project'}),\n" +
                "(nextGenPlatform {name:'Next Gen Platform', _label:'project'}),\n" +
                "ben-[:WORKS_FOR]->acme,\n" +
                "charlie-[:WORKS_FOR]->acme,\n" +
                "lucy-[:WORKS_FOR]->acme,\n" +
                "sarah-[:WORKS_FOR]->acme,\n" +
                "arnold-[:WORKS_FOR]->startup,\n" +
                "gordon-[:WORKS_FOR]->startup,\n" +
                "emily-[:WORKS_FOR]->startup,\n" +
                "kate-[:WORKS_FOR]->startup,\n" +
                "ben-[:INTERESTED_IN]->graphs,\n" +
                "ben-[:INTERESTED_IN]->rest,\n" +
                "arnold-[:INTERESTED_IN]->graphs,\n" +
                "arnold-[:INTERESTED_IN]->java,\n" +
                "arnold-[:INTERESTED_IN]->rest,\n" +
                "arnold-[:INTERESTED_IN]->travel,\n" +
                "charlie-[:INTERESTED_IN]->graphs,\n" +
                "charlie-[:INTERESTED_IN]->cars,\n" +
                "charlie-[:INTERESTED_IN]->medicine,\n" +
                "gordon-[:INTERESTED_IN]->graphs,\n" +
                "gordon-[:INTERESTED_IN]->art,\n" +
                "gordon-[:INTERESTED_IN]->music,\n" +
                "lucy-[:INTERESTED_IN]->art,\n" +
                "lucy-[:INTERESTED_IN]->drama,\n" +
                "lucy-[:INTERESTED_IN]->music,\n" +
                "emily-[:INTERESTED_IN]->art,\n" +
                "emily-[:INTERESTED_IN]->design,\n" +
                "sarah-[:INTERESTED_IN]->java,\n" +
                "sarah-[:INTERESTED_IN]->graphs,\n" +
                "sarah-[:INTERESTED_IN]->rest,\n" +
                "kate-[:INTERESTED_IN]->drama,\n" +
                "kate-[:INTERESTED_IN]->music,\n" +
                "arnold-[:WORKED_ON]->phoenix,\n" +
                "kate-[:WORKED_ON]->phoenix,\n" +
                "kate-[:WORKED_ON]->quantumLeap,\n" +
                "emily-[:WORKED_ON]->quantumLeap,\n" +
                "ben-[:WORKED_ON]->nextGenPlatform,\n" +
                "emily-[:WORKED_ON]->nextGenPlatform,\n" +
                "charlie-[:WORKED_ON]->nextGenPlatform,\n" +
                "sarah-[:WORKED_ON]->nextGenPlatform,\n" +
                "sarah-[:WORKED_ON]->quantumLeap";

        return createFromCypher(
                "Social Network",
                cypher,
                IndexParam.indexParam( "user", "name" ),
                IndexParam.indexParam( "topic", "name" ),
                IndexParam.indexParam( "project", "name" ) );

    }
}
