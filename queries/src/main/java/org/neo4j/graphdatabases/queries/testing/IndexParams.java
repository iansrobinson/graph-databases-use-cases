package org.neo4j.graphdatabases.queries.testing;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class IndexParams
{
    private final List<IndexParam> indexParams;

    public IndexParams( IndexParam... params )
    {
        indexParams = asList( params );
    }

    public void index(GraphDatabaseService db)
    {
        try (Transaction tx = db.beginTx()) {
            for (IndexParam indexParam : indexParams) {
                db.schema().indexFor(DynamicLabel.label(indexParam.nodeLabel())).on(indexParam.propertyName()).create();
//                db.schema().constraintFor(DynamicLabel.label(indexParam.nodeLabel())).assertPropertyIsUnique(indexParam.propertyName()).create();
//                engine.execute(format("CREATE INDEX ON :%s(%s)", indexParam.nodeLabel(), indexParam.propertyName()));
            }
            tx.success();
        }

    }
}
