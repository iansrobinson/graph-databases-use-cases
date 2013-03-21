package org.neo4j.graphdatabases.dataset_builders.properties;

import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class ProjectDurationGenerator
{
    private final static DateTime durationLowerLimit = new DateTime(2000, 1, 1, 0, 0, DateTimeZone.UTC);
    private final Random rand = new Random();

    public ProjectDuration getNextProjectDuration()
    {
        DateTime startDateTime = durationLowerLimit.plusMonths( rand.nextInt( 9 *12 ) );
        DateTime endDateTime = startDateTime.plusMonths( 3 + (rand.nextInt(33)) ) ;
        return new ProjectDuration( startDateTime.getMillis(), endDateTime.getMillis() );
    }
}
