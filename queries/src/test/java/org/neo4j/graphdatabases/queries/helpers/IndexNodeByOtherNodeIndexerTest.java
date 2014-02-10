package org.neo4j.graphdatabases.queries.helpers;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;

public class IndexNodeByOtherNodeIndexerTest
{
    @Test
    @Ignore("Doesn't work like that, the code used to index multiple children under one parent, not possible with the new indexes")
    public void shouldIndexNodesByOtherNodes() throws Exception
    {
        // given
        String cypher = "CREATE (a:Parent {name:'a'}), \n" +
                "(b:Child {name:'b'}), \n" +
                "(c:Child {name:'c'}), \n" +
                "(d:Child {name:'d'}), \n" +
                "(e:Child {name:'e'}), \n" +
                "(f:Child {name:'f'}),\n" +
                "(g:Child {name:'g'}),\n" +
                "(h:Child {name:'h'}),\n" +
                "a-[:CONNECTED_TO]->b,\n" +
                "a-[:CONNECTED_TO]->c,\n" +
                "a-[:CONNECTED_TO]->g,\n" +
                "d-[:CONNECTED_TO]->e,\n" +
                "d-[:CONNECTED_TO]->f,\n" +
                "d-[:CONNECTED_TO]->h";

        final GraphDatabaseService db = createFromCypher(
                "Example",
                cypher,
                IndexParam.indexParam( "Parent", "name" ) );
        ExecutionEngine executionEngine = new ExecutionEngine(db);

        IndexNodeByOtherNodeIndexer.GraphTraversal traversal1 = new IndexNodeByOtherNodeIndexer.GraphTraversal(){
            @Override
            public Iterable<Node> execute( Node startNode )
            {
                return GlobalGraphOperations.at(db).getAllNodesWithLabel(DynamicLabel.label("Parent"));
            }
        };

        IndexNodeByOtherNodeIndexer.GraphTraversal traversal2 = new IndexNodeByOtherNodeIndexer.GraphTraversal(){
                    @Override
                    public Iterable<Node> execute( Node startNode )
                    {
                        TraversalDescription traversalDescription = db.traversalDescription().breadthFirst().relationships(
                                withName( "CONNECTED_TO" ), Direction.OUTGOING ).evaluator( new Evaluator()
                        {
                            @Override
                            public Evaluation evaluate( Path path )
                            {
                                if ( path.endNode().equals( path.startNode() ) )
                                {
                                    return Evaluation.EXCLUDE_AND_CONTINUE;
                                }
                                return Evaluation.INCLUDE_AND_CONTINUE;
                            }
                        } );
                        return traversalDescription.traverse( startNode ).nodes();
                    }
                };

        IndexNodeByOtherNodeIndexer indexer = new IndexNodeByOtherNodeIndexer( traversal1, traversal2, "Parent", "child", "name" );

        // when
        indexer.execute( db, null, 2 );

        // then
        Map<String, String> indexValueToResult = new HashMap<>(  );
        indexValueToResult.put( "b", "a" );
        indexValueToResult.put( "c", "a" );
        indexValueToResult.put( "g", "a" );
        indexValueToResult.put( "e", "d" );
        indexValueToResult.put( "f", "d" );
        indexValueToResult.put( "h", "d" );

        for ( String indexValue : indexValueToResult.keySet() )
        {
            String query = "MATCH (n:Parent {child:'"+indexValue+"'}) RETURN n.name AS parent";

            ExecutionResult result = executionEngine.execute(query);
            assertEquals(indexValueToResult.get( indexValue ), result.iterator().next().get( "parent" ));
        }
    }
}
