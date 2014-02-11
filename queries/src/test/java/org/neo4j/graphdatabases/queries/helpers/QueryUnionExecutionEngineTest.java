package org.neo4j.graphdatabases.queries.helpers;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;
import static org.neo4j.graphdatabases.queries.testing.IndexParam.indexParam;

public class QueryUnionExecutionEngineTest
{
    private static final GraphDatabaseService database = createDatabase();
    private static ExecutionEngineWrapper executionEngine;
    private static Transaction transaction;

    @BeforeClass
    public static void init()
    {
        try
        {
            executionEngine = new ExecutionEngineWrapper()
            {
                private ExecutionEngine engine = new ExecutionEngine(database);

                @Override
                public ExecutionResult execute( String query, Map<String, Object> params )
                {
                    return engine.execute( query, params );
                }

                @Override
                public ExecutionResult execute( String query, Map<String, Object> params, int index )
                {
                    return execute( query, params );
                }
            };
        }
        catch ( Exception e )
        {
            System.out.println( e.getMessage() );
        }

        transaction = database.beginTx();
    }

    @AfterClass
    public static void end() {
        transaction.close();
        database.shutdown();
    }

    @Test
    public void shouldAllowIteratingSingleResult() throws Exception
    {
        // given
        String query =
                "MATCH (a:User {name:'a'})-[:FRIEND]->(person)\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // when
        Iterator<Map<String, Object>> results = queryUnionExecutionEngine.execute( query ).iterator();

        // then
        assertEquals( "b", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "c", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertFalse( results.hasNext() );
    }

    @Test
    public void shouldAllowIteratingMultipleResults() throws Exception
    {
        // given
        String query1 =
                "MATCH (a:User {name:'a'})-[:FRIEND]->(person)\n" +
                "RETURN person";
        String query2 =
                "MATCH (a:User {name:'a'})-[:ENEMY]->(person)\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // when
        Iterator<Map<String, Object>> results = queryUnionExecutionEngine.execute( query1, query2 ).iterator();

        // then
        assertEquals( "b", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "c", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "d", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "e", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertFalse( results.hasNext() );
    }

    @Test
    public void shouldReturnCorrectResultsWhenFirstQueryReturnsEmptyResults() throws Exception
    {
        // given
        String query1 =
                "MATCH (a:User {name:'a'})-[:COLLEAGUE]->(person)\n" +
                "RETURN person";
        String query2 =
                "MATCH (a:User {name:'a'})-[:ENEMY]->(person)\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // when
        Iterator<Map<String, Object>> results = queryUnionExecutionEngine.execute( query1, query2 ).iterator();

        // then
        assertEquals( "d", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "e", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertFalse( results.hasNext() );
    }

    @Test
    public void shouldReturnCorrectResultsWhenLastQueryReturnsEmptyResults() throws Exception
    {
        // given
        String query1 =
                "MATCH (a:User {name:'a'})-[:FRIEND]->(person)\n" +
                "RETURN person";
        String query2 =
                "MATCH (a:User {name:'a'})-[:COLLEAGUE]->(person)\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // when
        Iterator<Map<String, Object>> results = queryUnionExecutionEngine.execute( query1, query2 ).iterator();

        // then
        assertEquals( "b", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "c", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertFalse( results.hasNext() );
    }

    @Test
    public void shouldReturnCorrectResultsWhenMiddleQueryReturnsEmptyResults() throws Exception
    {
        // given
        String query1 =
                "MATCH (a:User {name:'a'})-[:FRIEND]->(person)\n" +
                "RETURN person";
        String query2 =
                "MATCH (a:User {name:'a'})-[:COLLEAGUE]->(person)\n" +
                "RETURN person";
        String query3 =
                "MATCH (a:User {name:'a'})-[:ENEMY]->(person)\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // when
        Iterator<Map<String, Object>> results = queryUnionExecutionEngine.execute( query1, query2, query3 ).iterator();

        // then
        assertEquals( "b", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "c", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "d", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "e", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertFalse( results.hasNext() );
    }

    @Test
    public void shouldReturnCorrectResultsWhenAllQueriesReturnEmptyResults() throws Exception
    {
        // given
        String query1 =
                "MATCH (a:User {name:'a'})-[:COLLEAGUE]->(person)\n" +
                "RETURN person";
        String query2 =
                "MATCH (a:User {name:'a'})-[:COLLEAGUE]->(person)\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // when
        Iterator<Map<String, Object>> results = queryUnionExecutionEngine.execute( query1, query2 ).iterator();

        // then
        assertFalse( results.hasNext() );
    }

    @Test
    public void shouldAllowSameParametersToBePassedToAllQueries() throws Exception
    {
        // given
        String query1 =
                "MATCH (a:User {name:'a'})-[:FRIEND]->(person)\n" +
                "RETURN person";
        String query2 =
                "MATCH (a:User {name:'a'})-[:ENEMY]->(person)\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", "a" );

        // when
        Iterator<Map<String, Object>> results = queryUnionExecutionEngine.execute( params, query1, query2 ).iterator();

        // then
        assertEquals( "b", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "c", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "d", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "e", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertFalse( results.hasNext() );
    }

    @Test
    public void shouldAllowDifferentParametersToBePassedToDifferentQueries() throws Exception
    {
        // given
        String query1 =
                "MATCH (a:User {name:'a'})-[:FRIEND]->(person)\n" +
                "RETURN person";
        String query2 =
                "MATCH (a:User {name:'a'})-[:ENEMY]->(person)\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", "a" );
        params.put( "user", "a" );

        // when
        Iterator<Map<String, Object>> results = queryUnionExecutionEngine.execute( params, query1, query2 ).iterator();

        // then
        assertEquals( "b", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "c", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "d", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertEquals( "e", ((Node) results.next().get( "person" )).getProperty( "name" ) );
        assertFalse( results.hasNext() );
    }

    @Test
    public void shouldThrowExceptionIfNoQueriesSupplied() throws Exception
    {
        // given
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // then
        try
        {
            queryUnionExecutionEngine.execute();
            fail( "Expected IllegalArgumentException" );
        }
        catch ( IllegalArgumentException ex )
        {
            assertEquals( "Must supply one or more queries.", ex.getMessage() );
        }
    }

    @Test
    public void shouldReturnAllResultsAsString() throws Exception
    {
        // given
        String query1 =
                "MATCH (a:User {name:'a'})-[:FRIEND]->(person)\n" +
                "RETURN person.name";
        String query2 =
                "MATCH (a:User {name:'a'})-[:ENEMY]->(person)\n" +
                "RETURN person.name";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // when
        QueryUnionExecutionResult results = (QueryUnionExecutionResult) queryUnionExecutionEngine.execute(
                query1,
                query2 );

        // then
        String resultsString = results.toString();
        assertTrue( resultsString.startsWith(
                "+-------------+\n" +
                        "| person.name |\n" +
                        "+-------------+\n" +
                        "| \"b\"         |\n" +
                        "| \"c\"         |\n" +
                        "+-------------+\n" +
                        "2 rows\n" ) );
        assertTrue( resultsString.contains(
                "+-------------+\n" +
                        "| person.name |\n" +
                        "+-------------+\n" +
                        "| \"d\"         |\n" +
                        "| \"e\"         |\n" +
                        "+-------------+\n" +
                        "2 rows\n" ) );
    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE\n" +
                "(a:User {name:'a'}),\n" +
                "(b:User {name:'b'}),\n" +
                "(c:User {name:'c'}),\n" +
                "(d:User {name:'d'}),\n" +
                "(e:User {name:'e'}),\n" +
                "(a)-[:FRIEND]->(b), (a)-[:FRIEND]->(c), (a)-[:ENEMY]->(d), (a)-[:ENEMY]->(e)";

        return createFromCypher(
                "Union example",
                cypher,
                indexParam( "User", "name" ) );
    }
}
