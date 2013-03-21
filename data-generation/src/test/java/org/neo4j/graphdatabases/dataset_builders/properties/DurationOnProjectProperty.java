package org.neo4j.graphdatabases.dataset_builders.properties;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.neode.properties.Property;

public class DurationOnProjectProperty extends Property
{
    @Override
    public void setProperty( PropertyContainer propertyContainer, GraphDatabaseService graphDatabaseService,
                             String s, int i )
    {
        Node endNode = ((Relationship) propertyContainer).getEndNode();

        Long startDateTime = (Long) endNode.getProperty( "start_date" );
        Long endDateTime = (Long) endNode.getProperty( "end_date" );
        ProjectDuration projectDuration = new ProjectDuration( startDateTime, endDateTime );
        ProjectDuration durationOnProject = projectDuration.getSubDuration();

        propertyContainer.setProperty( "duration", durationOnProject.toString() );
        propertyContainer.setProperty( "start_date", durationOnProject.getStartDateMs() );
        propertyContainer.setProperty( "end_date", durationOnProject.getEndDateMs() );
    }
}
