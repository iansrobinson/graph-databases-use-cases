package org.neo4j.graphdatabases.queries.helpers;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

public class IndexNodeByOtherNodeIndexerTest
{
    @Test
    public void shouldIndexNodesByOtherNodes() throws Exception
    {
        // given
        String cypher = "CREATE (a {name:'a', _label:'parent'}), \n" +
                "(b {name:'b', _label:'child'}), \n" +
                "(c {name:'c', _label:'child'}), \n" +
                "(d {name:'d', _label:'parent'}), \n" +
                "(e {name:'e', _label:'child'}), \n" +
                "(f {name:'f', _label:'child'}),\n" +
                "(g {name:'g', _label:'child'}),\n" +
                "(h {name:'h', _label:'child'}),\n" +
                "a-[:CONNECTED_TO]->b,\n" +
                "a-[:CONNECTED_TO]->c,\n" +
                "a-[:CONNECTED_TO]->g,\n" +
                "d-[:CONNECTED_TO]->e,\n" +
                "d-[:CONNECTED_TO]->f,\n" +
                "d-[:CONNECTED_TO]->h";

        final GraphDatabaseService db = createFromCypher(
                "Example",
                cypher,
                IndexParam.indexParam( "parent", "name" ) );

        IndexNodeByOtherNodeIndexer.GraphTraversal traversal1 = new IndexNodeByOtherNodeIndexer.GraphTraversal(){
            @Override
            public Iterable<Node> execute( Node startNode )
            {
                return db.index().forNodes( "parent" ).query( "name:*" );
            }
        };

        IndexNodeByOtherNodeIndexer.GraphTraversal traversal2 = new IndexNodeByOtherNodeIndexer.GraphTraversal(){
                    @Override
                    public Iterable<Node> execute( Node startNode )
                    {
                        TraversalDescription traversalDescription = Traversal.description().breadthFirst().relationships(
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

        IndexNodeByOtherNodeIndexer indexer = new IndexNodeByOtherNodeIndexer( traversal1, traversal2, "parent", "child", "name" );

        // when
        indexer.execute( db, null, 2 );

        // then
        Map<String, String> indexValueToResult = new HashMap<String, String>(  );
        indexValueToResult.put( "b", "a" );
        indexValueToResult.put( "c", "a" );
        indexValueToResult.put( "g", "a" );
        indexValueToResult.put( "e", "d" );
        indexValueToResult.put( "f", "d" );
        indexValueToResult.put( "h", "d" );

        for ( String indexValue : indexValueToResult.keySet() )
        {
            String query = "START n = node:parent(child='" + indexValue + "') RETURN n.name AS parent";
            ExecutionResult result = new ExecutionEngine( db ).execute( query );
            assertEquals(indexValueToResult.get( indexValue ), result.iterator().next().get( "parent" ));
        }
    }
}
