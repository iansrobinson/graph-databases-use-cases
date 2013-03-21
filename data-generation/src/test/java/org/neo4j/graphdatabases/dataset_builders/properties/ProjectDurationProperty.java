package org.neo4j.graphdatabases.dataset_builders.properties;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.neode.properties.Property;

public class ProjectDurationProperty extends Property
{
    private final ProjectDurationGenerator generator = new ProjectDurationGenerator();

    @Override
    public void setProperty( PropertyContainer propertyContainer, GraphDatabaseService graphDatabaseService,
                             String s, int i )
    {
        ProjectDuration projectDuration = generator.getNextProjectDuration();
        propertyContainer.setProperty( "duration", projectDuration.toString() );
        propertyContainer.setProperty( "start_date", projectDuration.getStartDateMs() );
        propertyContainer.setProperty( "end_date", projectDuration.getEndDateMs() );
    }
}
