package org.neo4j.graphdatabases.dataset_builders;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.neode.GraphQuery.traversal;
import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.TargetNodesStrategy.getExisting;
import static org.neo4j.neode.TargetNodesStrategy.getOrCreate;
import static org.neo4j.neode.TargetNodesStrategy.queryBasedGetOrCreate;
import static org.neo4j.neode.probabilities.ProbabilityDistribution.flatDistribution;
import static org.neo4j.neode.probabilities.ProbabilityDistribution.normalDistribution;
import static org.neo4j.neode.properties.Property.indexableProperty;
import static org.neo4j.neode.properties.Property.property;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdatabases.SocialNetwork;
import org.neo4j.graphdatabases.dataset_builders.properties.DurationOnProjectProperty;
import org.neo4j.graphdatabases.dataset_builders.properties.ProjectDurationProperty;
import org.neo4j.graphdatabases.dataset_builders.traversers.FindAllColleagues;
import org.neo4j.graphdatabases.dataset_builders.traversers.IsCompanyProject;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.neode.Dataset;
import org.neo4j.neode.DatasetManager;
import org.neo4j.neode.NodeCollection;
import org.neo4j.neode.NodeSpecification;
import org.neo4j.neode.RelationshipSpecification;
import org.neo4j.neode.RelationshipUniqueness;
import org.neo4j.neode.logging.SysOutLog;
import org.neo4j.neode.properties.Property;
import org.neo4j.neode.statistics.AsciiDocFormatter;
import org.neo4j.neode.statistics.GraphStatistics;

@Ignore
public class BuildSocialNetworkGraph
{
    @Test
    public void buildSocialNetwork() throws Exception
    {
        File dir = new File( SocialNetwork.STORE_DIR );
        FileUtils.deleteRecursively( dir );

        Map<String, String> params = new HashMap<String, String>();
        params.put( "dump_configuration", "true" );
        params.put( "cache_type", "gcr" );

        GraphDatabaseService db = new EmbeddedGraphDatabase( SocialNetwork.STORE_DIR, params );
        DatasetManager dsm = new DatasetManager( db, SysOutLog.INSTANCE );

        TraversalDescription findCompanyProjects = createFindCompanyProjectsTraversalDescription();
        Property projectDuration = new ProjectDurationProperty();
        Property durationOnProject = new DurationOnProjectProperty();

        NodeSpecification userSpec = dsm.nodeSpecification( "user", indexableProperty( "name" ) );
        NodeSpecification topicSpec = dsm.nodeSpecification( "topic", indexableProperty( "name" ) );
        NodeSpecification companySpec = dsm.nodeSpecification( "company", indexableProperty( "name" ) );
        NodeSpecification projectSpec = dsm.nodeSpecification( "project",
                property( "name" ),
                projectDuration );

        RelationshipSpecification interested_in = dsm.relationshipSpecification( "INTERESTED_IN" );
        RelationshipSpecification works_for = dsm.relationshipSpecification( "WORKS_FOR" );
        RelationshipSpecification worked_on = dsm.relationshipSpecification( "WORKED_ON", durationOnProject );
        RelationshipSpecification worked_with = dsm.relationshipSpecification( "WORKED_WITH" );

        Dataset dataset = dsm.newDataset( "Social network example" );

        NodeCollection users = userSpec.create( SocialNetwork.NUMBER_USERS ).update( dataset );

        users.createRelationshipsTo(
                getOrCreate( topicSpec, SocialNetwork.NUMBER_TOPICS, normalDistribution() )
                        .numberOfTargetNodes( minMax( 1, 3 ) )
                        .relationship( interested_in )
                        .exactlyOneRelationship() )
                .updateNoReturn( dataset );

        users.createRelationshipsTo(
                getOrCreate( companySpec, SocialNetwork.NUMBER_COMPANIES, flatDistribution() )
                        .numberOfTargetNodes( 1 )
                        .relationship( works_for )
                        .exactlyOneRelationship() )
                .updateNoReturn( dataset );

        NodeCollection allProjects = users.createRelationshipsTo(
                queryBasedGetOrCreate( projectSpec, traversal( findCompanyProjects ), 5.0 )
                        .numberOfTargetNodes( minMax( 1, 5 ), normalDistribution() )
                        .relationship( worked_on )
                        .exactlyOneRelationship() )
                .update( dataset );

        users.approxPercentage( 30 ).createRelationshipsTo(
                getExisting( allProjects )
                        .numberOfTargetNodes( minMax( 1, 2 ), normalDistribution() )
                        .relationship( worked_on )
                        .relationshipConstraints( RelationshipUniqueness.BOTH_DIRECTIONS ) )
                .update( dataset );

        users.createRelationshipsTo(
                getExisting( new FindAllColleagues( ) )
                        .numberOfTargetNodes( 1 )
                        .relationship( worked_with )
                        .exactlyOneRelationship() )
                .updateNoReturn( dataset, 5000 );


        dataset.end();

        GraphStatistics.create( db, SocialNetwork.TITLE ).describeTo(
                new AsciiDocFormatter( SysOutLog.INSTANCE ) );

        db.shutdown();
    }

    private TraversalDescription createFindCompanyProjectsTraversalDescription()
    {
        return Traversal.description()
                .depthFirst()
                .uniqueness( Uniqueness.NODE_GLOBAL )
                .relationships( withName( "WORKS_FOR" ), Direction.BOTH )
                .relationships( withName( "WORKED_ON" ), Direction.OUTGOING )
                .evaluator( new IsCompanyProject() );
    }
}
