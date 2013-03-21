package org.neo4j.graphdatabases.queries.helpers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;
import static org.neo4j.graphdatabases.queries.testing.IndexParam.indexParam;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class QueryUnionExecutionEngineTest
{
    private static ExecutionEngineWrapper executionEngine;

    @BeforeClass
    public static void init()
    {
        try
        {
            executionEngine = new ExecutionEngineWrapper()
            {
                private ExecutionEngine engine = new ExecutionEngine( createDatabase() );

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
    }

    @Test
    public void shouldAllowIteratingSingleResult() throws Exception
    {
        // given
        String query = "START a = node:user(name='a')\n" +
                "MATCH a-[:FRIEND]->person\n" +
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
        String query1 = "START a = node:user(name='a')\n" +
                "MATCH a-[:FRIEND]->person\n" +
                "RETURN person";
        String query2 = "START a = node:user(name='a')\n" +
                "MATCH a-[:ENEMY]->person\n" +
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
        String query1 = "START a = node:user(name='a')\n" +
                "MATCH a-[:COLLEAGUE]->person\n" +
                "RETURN person";
        String query2 = "START a = node:user(name='a')\n" +
                "MATCH a-[:ENEMY]->person\n" +
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
        String query1 = "START a = node:user(name='a')\n" +
                "MATCH a-[:FRIEND]->person\n" +
                "RETURN person";
        String query2 = "START a = node:user(name='a')\n" +
                "MATCH a-[:COLLEAGUE]->person\n" +
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
        String query1 = "START a = node:user(name='a')\n" +
                "MATCH a-[:FRIEND]->person\n" +
                "RETURN person";
        String query2 = "START a = node:user(name='a')\n" +
                "MATCH a-[:COLLEAGUE]->person\n" +
                "RETURN person";
        String query3 = "START a = node:user(name='a')\n" +
                "MATCH a-[:ENEMY]->person\n" +
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
        String query1 = "START a = node:user(name='a')\n" +
                "MATCH a-[:COLLEAGUE]->person\n" +
                "RETURN person";
        String query2 = "START a = node:user(name='a')\n" +
                "MATCH a-[:COLLEAGUE]->person\n" +
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
        String query1 = "START a = node:user(name={name})\n" +
                "MATCH a-[:FRIEND]->person\n" +
                "RETURN person";
        String query2 = "START a = node:user(name={name})\n" +
                "MATCH a-[:ENEMY]->person\n" +
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
        String query1 = "START a = node:user(name={name})\n" +
                "MATCH a-[:FRIEND]->person\n" +
                "RETURN person";
        String query2 = "START a = node:user(name={user})\n" +
                "MATCH a-[:ENEMY]->person\n" +
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
        String query1 = "START a = node:user(name='a')\n" +
                "MATCH a-[:FRIEND]->person\n" +
                "RETURN person";
        String query2 = "START a = node:user(name='a')\n" +
                "MATCH a-[:ENEMY]->person\n" +
                "RETURN person";
        QueryUnionExecutionEngine queryUnionExecutionEngine = new QueryUnionExecutionEngine( executionEngine );

        // when
        QueryUnionExecutionResult results = (QueryUnionExecutionResult) queryUnionExecutionEngine.execute(
                query1,
                query2 );

        // then
        String resultsString = results.toString();
        System.out.println(resultsString);
        assertTrue( resultsString.startsWith(
                "+---------------------------------+\n" +
                        "| person                          |\n" +
                        "+---------------------------------+\n" +
                        "| Node[2]{name:\"b\",_label:\"user\"} |\n" +
                        "| Node[3]{name:\"c\",_label:\"user\"} |\n" +
                        "+---------------------------------+\n" ) );
        assertTrue( resultsString.contains(
                "+---------------------------------+\n" +
                        "| person                          |\n" +
                        "+---------------------------------+\n" +
                        "| Node[4]{name:\"d\",_label:\"user\"} |\n" +
                        "| Node[5]{name:\"e\",_label:\"user\"} |\n" +
                        "+---------------------------------+\n" ) );
    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE\n" +
                "a = {name:'a', _label:'user'},\n" +
                "b = {name:'b', _label:'user'},\n" +
                "c = {name:'c', _label:'user'},\n" +
                "d = {name:'d', _label:'user'},\n" +
                "e = {name:'e', _label:'user'},\n" +
                "a-[:FRIEND]->b, a-[:FRIEND]->c, a-[:ENEMY]->d, a-[:ENEMY]->e";

        return createFromCypher(
                "Union example",
                cypher,
                indexParam( "user", "name" ) );
    }
}
