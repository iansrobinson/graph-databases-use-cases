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
    public TestName name = new TestName();

    @Test
    public void suspectBehaviour() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        EmailQueries queries = new EmailQueries( db, new PrintingExecutionEngineWrapper( db, "email", name ) );

        ExecutionResult result = queries.suspectBehaviour();
        Iterator<Map<String, Object>> iterator = result.iterator();
        Map<String, Object> next = iterator.next();
        assertEquals( "1", next.get( "email.id" ));
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
        String cypher = "CREATE \n" +
                "(charlie:User {username:'Charlie'}),\n" +
                "(davina:User {username:'Davina'}),\n" +
                "(edward:User {username:'Edward'}),\n" +
                "(alice:User {username:'Alice'}),\n" +
                "(bob:User {username:'Bob'}),\n" +
                "(alice)-[:ALIAS_OF]->(bob),\n" +
                "\n" +
                "(email_1:Email {id: '1', content: 'email contents'}),\n" +
                "(bob)-[:SENT]->(email_1),\n" +
                "(email_1)-[:TO]->(charlie),\n" +
                "(email_1)-[:CC]->(davina),\n" +
                "(email_1)-[:CC]->(alice),\n" +
                "(email_1)-[:BCC]->(edward),\n" +
                "\n" +
                "(email_2:Email {id: '2', content: 'email contents'}),\n" +
                "(bob)-[:SENT]->(email_2),\n" +
                "(email_2)-[:TO]->(davina),\n" +
                "(email_2)-[:BCC]->(edward),\n" +
                "\n" +
                "(email_3:Email {id: '3', content: 'email contents'}),\n" +
                "(davina)-[:SENT]->(email_3),\n" +
                "(email_3)-[:TO]->(bob),\n" +
                "(email_3)-[:CC]->(edward),\n" +
                "\n" +
                "(email_4:Email {id: '4', content: 'email contents'}),\n" +
                "(charlie)-[:SENT]->(email_4),\n" +
                "(email_4)-[:TO]->(bob),\n" +
                "(email_4)-[:TO]->(davina),\n" +
                "(email_4)-[:TO]->(edward),\n" +
                "\n" +
                "(email_5:Email {id: '5', content: 'email contents'}),\n" +
                "(davina)-[:SENT]->(email_5),\n" +
                "(email_5)-[:TO]->(alice),\n" +
                "(email_5)-[:BCC]->(bob),\n" +
                "(email_5)-[:BCC]->(edward)";

        return createFromCypher(
                "Email",
                cypher,
                IndexParam.indexParam( "User", "username" ),
                IndexParam.indexParam( "Email", "id" )
        );
    }

    private static GraphDatabaseService createDatabase2()
    {
        String cypher = "CREATE \n" +
                "(charlie:User {username:'Charlie'}),\n" +
                "(davina:User {username:'Davina'}),\n" +
                "(edward:User {username:'Edward'}),\n" +
                "(alice:User {username:'Alice'}),\n" +
                "(bob:User {username:'Bob'}),\n" +
                "(alice)-[:ALIAS_OF]->(bob),\n" +
                "\n" +
                "(email_6:Email {id: '6', content: 'email'}),\n" +
                "(bob)-[:SENT]->(email_6),\n" +
                "(email_6)-[:TO]->(charlie),\n" +
                "(email_6)-[:TO]->(davina),\n" +
                "\n" +
                "(reply_1:Email {id: '7', content: 'response'}),\n" +
                "(reply_1)-[:REPLY_TO]->(email_6),\n" +
                "(davina)-[:SENT]->(reply_1),\n" +
                "(reply_1)-[:TO]->(bob),\n" +
                "(reply_1)-[:TO]->(charlie),\n" +
                "\n" +
                "(reply_2:Email {id: '8', content: 'response'}),\n" +
                "(reply_2)-[:REPLY_TO]->(email_6),\n" +
                "(bob)-[:SENT]->(reply_2),\n" +
                "(reply_2)-[:TO]->(davina),\n" +
                "(reply_2)-[:TO]->(charlie),\n" +
                "(reply_2)-[:CC]->(alice),\n" +
                "\n" +
                "(reply_3:Email {id: '9', content: 'response'}),\n" +
                "(reply_3)-[:REPLY_TO]->(reply_1),\n" +
                "(charlie)-[:SENT]->(reply_3),\n" +
                "(reply_3)-[:TO]->(bob),\n" +
                "(reply_3)-[:TO]->(davina),\n" +
                "\n" +
                "(reply_4:Email {id: '10', content: 'response'}),\n" +
                "(reply_4)-[:REPLY_TO]->(reply_3),\n" +
                "(bob)-[:SENT]->(reply_4),\n" +
                "(reply_4)-[:TO]->(charlie),\n" +
                "(reply_4)-[:TO]->(davina)";

        return createFromCypher(
                "Email",
                cypher,
                IndexParam.indexParam( "User", "username" ),
                IndexParam.indexParam( "Email", "id" )
        );
    }

    private static GraphDatabaseService createDatabase3()
    {
        String cypher = "CREATE \n" +
                "(charlie:User {username:'Charlie'}),\n" +
                "(davina:User {username:'Davina'}),\n" +
                "(edward:User {username:'Edward'}),\n" +
                "(alice:User {username:'Alice'}),\n" +
                "(bob:User {username:'Bob'}),\n" +
                "(alice)-[:ALIAS_OF]->(bob),\n" +
                "\n" +
                "(email_11:Email {id: '11', content: 'email'}),\n" +
                "       (alice)-[:SENT]->(email_11)-[:TO]->(bob),\n" +
                "\n" +
                "(email_12:Email {id: '12', content: 'email'}),\n" +
                "       (email_12)-[:FORWARD_OF]->(email_11),\n" +
                "       (bob)-[:SENT]->(email_12)-[:TO]->(charlie),\n" +
                "\n" +
                "(email_13:Email {id: '13', content: 'email'}),\n" +
                "       (email_13)-[:FORWARD_OF]->(email_12),\n" +
                "       (charlie)-[:SENT]->(email_13)-[:TO]->(davina)";

        return createFromCypher(
                "Email",
                cypher,
                IndexParam.indexParam( "User", "username" ),
                IndexParam.indexParam( "Email", "id" )
        );
    }

    private static GraphDatabaseService createDatabase4()
    {
        String cypher = "CREATE (alice:User {username: 'Alice'}),\n" +
                "(bob:User {username: 'Bob'}),\n" +
                "(charlie:User {username: 'Charlie'}),\n" +
                "(davina:User {username: 'Davina'}),\n" +
                "(edward:User {username: 'Edward'}),\n" +
                "(alice)-[:ALIAS_OF]->(bob),\n" +
                "(bob)-[:EMAILED]->(charlie),\n" +
                "(bob)-[:CC]->(davina),\n" +
                "(bob)-[:BCC]->(edward)";

        return createFromCypher(
                "Email",
                cypher,
                IndexParam.indexParam( "User", "username" )
        );
    }
}
