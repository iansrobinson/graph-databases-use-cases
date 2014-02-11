package org.neo4j.graphdatabases.dataset_builders;

import java.io.File;
import java.util.Random;

import org.junit.Test;

import org.neo4j.graphdatabases.AccessControlWithRelationshipPropertiesConfig;
import org.neo4j.graphdatabases.queries.helpers.DbUtils;
import org.neo4j.graphdatabases.queries.traversals.IndexResources;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.neode.Dataset;
import org.neo4j.neode.DatasetManager;
import org.neo4j.neode.NodeCollection;
import org.neo4j.neode.NodeSpecification;
import org.neo4j.neode.Range;
import org.neo4j.neode.RelationshipSpecification;
import org.neo4j.neode.RelationshipUniqueness;
import org.neo4j.neode.logging.SysOutLog;
import org.neo4j.neode.properties.Property;
import org.neo4j.neode.statistics.AsciiDocFormatter;
import org.neo4j.neode.statistics.GraphStatistics;

import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.TargetNodesStrategy.create;
import static org.neo4j.neode.TargetNodesStrategy.getExisting;
import static org.neo4j.neode.TargetNodesStrategy.getOrCreate;
import static org.neo4j.neode.properties.Property.indexableProperty;
import static org.neo4j.neode.properties.Property.property;

public class AccessControlWithRelationshipProperties
{
    public static final Range GROUPS_PER_ADMIN = minMax( 1, 3 );
    public static final Range ALLOWED_COMPANIES_PER_GROUP = minMax( 10, 50 );
    public static final Range DENIED_COMPANIES_PER_GROUP = minMax( 2, 10 );
    public static final Range EMPLOYEES_PER_COMPANY = minMax( 5, 100 );
    public static final Range ACCOUNTS_PER_EMPLOYEE = minMax( 1, 5 );

    @Test
    public void buildAccessControl() throws Exception
    {
        File dir = new File( AccessControlWithRelationshipPropertiesConfig.STORE_DIR );
        FileUtils.deleteRecursively( dir );

        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( AccessControlWithRelationshipPropertiesConfig.STORE_DIR )
                .setConfig( DbUtils.dbConfig() )
                .newGraphDatabase();
        DatasetManager dsm = new DatasetManager( db, SysOutLog.INSTANCE );

        NodeSpecification adminSpec = dsm.nodeSpecification( "Administrator", indexableProperty( db, "Administrator", "name" ) );
        NodeSpecification groupSpec = dsm.nodeSpecification( "Group", property( "name" ) );
        NodeSpecification companySpec = dsm.nodeSpecification( "Company", indexableProperty( db, "Company", "name" ) );
        NodeSpecification customerSpec = dsm.nodeSpecification( "Employee", indexableProperty( db, "Employee", "name", "Resource"));
        NodeSpecification accountSpec = dsm.nodeSpecification( "Account", indexableProperty( db, "Account", "name", "Resource"));

        Property inheritProperty = new Property()
        {
            private final Random random = new Random();

            @Override
            public void setProperty( PropertyContainer propertyContainer, GraphDatabaseService graphDatabaseService,
                                     String label, int iteration )
            {
                int i = random.nextInt( 3 );
                boolean value = i < 2;
                propertyContainer.setProperty( "inherit", value );
            }
        };

        RelationshipSpecification member_of = dsm.relationshipSpecification( "MEMBER_OF" );
        RelationshipSpecification allowed = dsm.relationshipSpecification( "ALLOWED", inheritProperty );
        RelationshipSpecification denied = dsm.relationshipSpecification( "DENIED" );
        RelationshipSpecification child_of = dsm.relationshipSpecification( "CHILD_OF" );
        RelationshipSpecification works_for = dsm.relationshipSpecification( "WORKS_FOR" );
        RelationshipSpecification has_account = dsm.relationshipSpecification( "HAS_ACCOUNT" );

        Dataset dataset = dsm.newDataset( AccessControlWithRelationshipPropertiesConfig.TITLE );

        NodeCollection administrators = adminSpec.create( AccessControlWithRelationshipPropertiesConfig
                .NUMBER_OF_ADMINS ).update( dataset );
        NodeCollection groups = administrators.createRelationshipsTo(
                getOrCreate( groupSpec, AccessControlWithRelationshipPropertiesConfig.NUMBER_OF_GROUPS )
                        .numberOfTargetNodes( GROUPS_PER_ADMIN )
                        .relationship( member_of )
                        .relationshipConstraints( RelationshipUniqueness.BOTH_DIRECTIONS ) )
                .update( dataset );

        NodeCollection companies1allowed = groups.createRelationshipsTo(
                getOrCreate( companySpec, percentageOf( AccessControlWithRelationshipPropertiesConfig
                        .NUMBER_OF_COMPANIES, 0.25 ) )
                        .numberOfTargetNodes( ALLOWED_COMPANIES_PER_GROUP )
                        .relationship( allowed )
                        .relationshipConstraints( RelationshipUniqueness.BOTH_DIRECTIONS ) )
                .update( dataset );

        NodeCollection companies1denied = groups.createRelationshipsTo(
                getOrCreate( companySpec, percentageOf( AccessControlWithRelationshipPropertiesConfig
                        .NUMBER_OF_COMPANIES, 0.1 ) )
                        .numberOfTargetNodes( DENIED_COMPANIES_PER_GROUP )
                        .relationship( denied )
                        .relationshipConstraints( RelationshipUniqueness.BOTH_DIRECTIONS ) )
                .update( dataset );

        NodeCollection companies2allowed = groups.createRelationshipsTo(
                getOrCreate( companySpec, percentageOf( AccessControlWithRelationshipPropertiesConfig
                        .NUMBER_OF_COMPANIES, 0.50 ) )
                        .numberOfTargetNodes( ALLOWED_COMPANIES_PER_GROUP )
                        .relationship( allowed )
                        .relationshipConstraints( RelationshipUniqueness.BOTH_DIRECTIONS ) )
                .update( dataset );

        NodeCollection companies2denied = groups.createRelationshipsTo(
                getOrCreate( companySpec, percentageOf( AccessControlWithRelationshipPropertiesConfig
                        .NUMBER_OF_COMPANIES, 0.15 ) )
                        .numberOfTargetNodes( DENIED_COMPANIES_PER_GROUP )
                        .relationship( denied )
                        .relationshipConstraints( RelationshipUniqueness.BOTH_DIRECTIONS ) )
                .update( dataset );

        NodeCollection companies1 = companies1allowed.combine( companies1denied );
        NodeCollection companies2 = companies2allowed.combine( companies2denied );

        companies2.createRelationshipsTo(
                getExisting( companies1 )
                        .numberOfTargetNodes( 1 )
                        .relationship( child_of )
                        .exactlyOneRelationship() )
                .updateNoReturn( dataset );


        NodeCollection companies = companies1.combine( companies2 );
        NodeCollection employees = companies.createRelationshipsTo(
                create( customerSpec )
                        .numberOfTargetNodes( EMPLOYEES_PER_COMPANY )
                        .relationship( works_for, Direction.INCOMING )
                        .exactlyOneRelationship() )
                .update( dataset, 1000 );

        employees.createRelationshipsTo(
                create( accountSpec )
                        .numberOfTargetNodes( ACCOUNTS_PER_EMPLOYEE )
                        .relationship( has_account, Direction.OUTGOING )
                        .exactlyOneRelationship() )
                .updateNoReturn( dataset );

        dataset.end();

        new IndexResources( db ).execute();

        GraphStatistics.create( db, AccessControlWithRelationshipPropertiesConfig.TITLE )
                .describeTo( new AsciiDocFormatter( SysOutLog.INSTANCE ) );

        db.shutdown();

    }

    private int percentageOf( int i, double percentage )
    {
        return (int) (i * percentage);
    }

}
