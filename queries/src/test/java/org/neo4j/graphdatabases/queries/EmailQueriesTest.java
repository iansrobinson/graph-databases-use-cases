package org.neo4j.graphdatabases.queries;

import java.util.Iterator;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.PrintingExecutionEngineWrapper;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class EmailQueriesTest
{
    @Rule
    public static TestName name = new TestName();

    @Test
    public void suspectBehaviour() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        EmailQueries queries = new EmailQueries( db, new PrintingExecutionEngineWrapper( db, "email", name ) );

        ExecutionResult result = queries.suspectBehaviour();
        Iterator<Map<String, Object>> iterator = result.iterator();
        Map<String, Object> next = iterator.next();
        assertEquals( "1", ((Node) next.get( "email" )).getProperty( "id" ) );
        assertFalse( iterator.hasNext() );

        db.shutdown();
    }

    @Test
    public void suspectBehaviour2() throws Exception
    {
        GraphDatabaseService db = createDatabase2();
        EmailQueries queries = new EmailQueries( db, new PrintingExecutionEngineWrapper( db, "email", name ) );

        ExecutionResult result = queries.suspectBehaviour2();

        Iterator<Map<String, Object>> iterator = result.iterator();
        Map<String, Object> next = iterator.next();
        assertEquals( 1L, next.get( "depth" ) );
        assertEquals( "Davina", next.get( "replier" ) );

        next = iterator.next();
        assertEquals( 1L, next.get( "depth" ) );
        assertEquals( "Bob", next.get( "replier" ) );

        next = iterator.next();
        assertEquals( 2L, next.get( "depth" ) );
        assertEquals( "Charlie", next.get( "replier" ) );

        next = iterator.next();
        assertEquals( 3L, next.get( "depth" ) );
        assertEquals( "Bob", next.get( "replier" ) );

        assertFalse( iterator.hasNext() );


        db.shutdown();
    }

    @Test
    public void suspectBehaviour3() throws Exception
    {
        GraphDatabaseService db = createDatabase3();
        EmailQueries queries = new EmailQueries( db, new PrintingExecutionEngineWrapper( db, "email", name ) );

        ExecutionResult result = queries.suspectBehaviour3();

        Iterator<Object> objectIterator = result.columnAs( "count(f)" );
        assertEquals( 2L, objectIterator.next() );

        assertFalse( objectIterator.hasNext() );

        db.shutdown();
    }

    @Test
    public void lossyDb() throws Exception
    {
        GraphDatabaseService db = createDatabase4();
        EmailQueries queries = new EmailQueries( db, new PrintingExecutionEngineWrapper( db, "email", name ) );

        ExecutionResult result = queries.lossyDb();

        assertEquals(1, count(result.iterator()));

        db.shutdown();
    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE (bob {username:'Bob', _label:'user'}),\n" +
                "(charlie {username:'Charlie', _label:'user'}),\n" +
                "(davina {username:'Davina', _label:'user'}),\n" +
                "(edward {username:'Edward', _label:'user'}),\n" +
                "(alice {username:'Alice', _label:'user'}),\n" +
                "(bob {username:'Bob', _label:'user'}),\n" +
                "(alice)-[:ALIAS_OF]->(bob),\n" +
                "\n" +
                "(email_1 {id: '1', content: 'email contents', _label:'email'}),\n" +
                "(bob)-[:SENT]->(email_1),\n" +
                "(email_1)-[:TO]->(charlie),\n" +
                "(email_1)-[:CC]->(davina),\n" +
                "(email_1)-[:CC]->(alice),\n" +
                "(email_1)-[:BCC]->(edward),\n" +
                "\n" +
                "(email_2 {id: '2', content: 'email contents', _label:'email'}),\n" +
                "(bob)-[:SENT]->(email_2),\n" +
                "(email_2)-[:TO]->(davina),\n" +
                "(email_2)-[:BCC]->(edward),\n" +
                "\n" +
                "(email_3 {id: '3', content: 'email contents', _label:'email'}),\n" +
                "(davina)-[:SENT]->(email_3),\n" +
                "(email_3)-[:TO]->(bob),\n" +
                "(email_3)-[:CC]->(edward),\n" +
                "\n" +
                "(email_4 {id: '4', content: 'email contents', _label:'email'}),\n" +
                "(charlie)-[:SENT]->(email_4),\n" +
                "(email_4)-[:TO]->(bob),\n" +
                "(email_4)-[:TO]->(davina),\n" +
                "(email_4)-[:TO]->(edward),\n" +
                "\n" +
                "(email_5 {id: '5', content: 'email contents', _label:'email'}),\n" +
                "(davina)-[:SENT]->(email_5),\n" +
                "(email_5)-[:TO]->(alice),\n" +
                "(email_5)-[:BCC]->(bob),\n" +
                "(email_5)-[:BCC]->(edward)";

        return createFromCypher(
                "Email",
                cypher,
                IndexParam.indexParam( "user", "username" ),
                IndexParam.indexParam( "email", "id" )
        );
    }

    private static GraphDatabaseService createDatabase2()
    {
        String cypher = "CREATE (bob {username:'Bob', _label:'user'}),\n" +
                "(charlie {username:'Charlie', _label:'user'}),\n" +
                "(davina {username:'Davina', _label:'user'}),\n" +
                "(edward {username:'Edward', _label:'user'}),\n" +
                "(alice {username:'Alice', _label:'user'}),\n" +
                "(bob {username:'Bob', _label:'user'}),\n" +
                "(alice)-[:ALIAS_OF]->(bob),\n" +
                "\n" +
                "(email_6 {id: '6', content: 'email', _label:'email'}),\n" +
                "(bob)-[:SENT]->(email_6),\n" +
                "(email_6)-[:TO]->(charlie),\n" +
                "(email_6)-[:TO]->(davina),\n" +
                "\n" +
                "(reply_1 {id: '7', content: 'response', _label:'email'}),\n" +
                "(reply_1)-[:REPLY_TO]->(email_6),\n" +
                "(davina)-[:SENT]->(reply_1),\n" +
                "(reply_1)-[:TO]->(bob),\n" +
                "(reply_1)-[:TO]->(charlie),\n" +
                "\n" +
                "(reply_2 {id: '8', content: 'response', _label:'email'}),\n" +
                "(reply_2)-[:REPLY_TO]->(email_6),\n" +
                "(bob)-[:SENT]->(reply_2),\n" +
                "(reply_2)-[:TO]->(davina),\n" +
                "(reply_2)-[:TO]->(charlie),\n" +
                "(reply_2)-[:CC]->(alice),\n" +
                "\n" +
                "(reply_3 {id: '9', content: 'response', _label:'email'}),\n" +
                "(reply_3)-[:REPLY_TO]->(reply_1),\n" +
                "(charlie)-[:SENT]->(reply_3),\n" +
                "(reply_3)-[:TO]->(bob),\n" +
                "(reply_3)-[:TO]->(davina),\n" +
                "\n" +
                "(reply_4 {id: '10', content: 'response', _label:'email'}),\n" +
                "(reply_4)-[:REPLY_TO]->(reply_3),\n" +
                "(bob)-[:SENT]->(reply_4),\n" +
                "(reply_4)-[:TO]->(charlie),\n" +
                "(reply_4)-[:TO]->(davina)";

        return createFromCypher(
                "Email",
                cypher,
                IndexParam.indexParam( "user", "username" ),
                IndexParam.indexParam( "email", "id" )
        );
    }

    private static GraphDatabaseService createDatabase3()
    {
        String cypher = "CREATE (bob {username:'Bob', _label:'user'}),\n" +
                "(charlie {username:'Charlie', _label:'user'}),\n" +
                "(davina {username:'Davina', _label:'user'}),\n" +
                "(edward {username:'Edward', _label:'user'}),\n" +
                "(alice {username:'Alice', _label:'user'}),\n" +
                "(bob {username:'Bob', _label:'user'}),\n" +
                "(alice)-[:ALIAS_OF]->(bob),\n" +
                "\n" +
                "(email_11 {id: '11', content: 'email', _label:'email'}),\n" +
                "       (alice)-[:SENT]->(email_11)-[:TO]->(bob),\n" +
                "\n" +
                "(email_12 {id: '12', content: 'email', _label:'email'}),\n" +
                "       (email_12)-[:FORWARD_OF]->(email_11),\n" +
                "       (bob)-[:SENT]->(email_12)-[:TO]->(charlie),\n" +
                "\n" +
                "(email_13 {id: '13', content: 'email', _label:'email'}),\n" +
                "       (email_13)-[:FORWARD_OF]->(email_12),\n" +
                "       (charlie)-[:SENT]->(email_13)-[:TO]->(davina)";

        return createFromCypher(
                "Email",
                cypher,
                IndexParam.indexParam( "user", "username" ),
                IndexParam.indexParam( "email", "id" )
        );
    }

    private static GraphDatabaseService createDatabase4()
    {
        String cypher = "CREATE (alice {username: 'Alice', _label:'user'}),\n" +
                "(bob {username: 'Bob', _label:'user'}),\n" +
                "(charlie {username: 'Charlie', _label:'user'}),\n" +
                "(davina {username: 'Davina', _label:'user'}),\n" +
                "(edward {username: 'Edward', _label:'user'}),\n" +
                "(alice)-[:ALIAS_OF]->(bob),\n" +
                "(bob)-[:EMAILED]->(charlie),\n" +
                "(bob)-[:CC]->(davina),\n" +
                "(bob)-[:BCC]->(edward)";

        return createFromCypher(
                "Email",
                cypher,
                IndexParam.indexParam( "user", "username" )
        );
    }
}
