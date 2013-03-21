package org.neo4j.graphdatabases.dataset_builders.traversers;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class IsCompanyProject implements Evaluator
{
    private static final RelationshipType WORKED_ON = withName( "WORKED_ON" );

    @Override
    public Evaluation evaluate( Path path )
    {
        if ( path.length() == 0 )
        {
            return Evaluation.EXCLUDE_AND_CONTINUE;
        }
        if ( path.lastRelationship().isType( WORKED_ON ) )
        {
            return Evaluation.INCLUDE_AND_PRUNE;
        }
        return Evaluation.EXCLUDE_AND_CONTINUE;
    }
}
