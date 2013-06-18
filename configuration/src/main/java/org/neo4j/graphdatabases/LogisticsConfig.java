package org.neo4j.graphdatabases;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class LogisticsConfig
{
    public static final String TITLE = "Logistics";
    public static final String STORE_DIR = "../datasets/logistics/";
    public static final DateTime START_DATE = new DateTime( 2012, 10, 15, 0, 0, 0, 0, DateTimeZone.UTC );
    public static final int NUMBER_OF_PARCEL_CENTRES = 5;
    public static final int MIN_NUMBER_OF_DELIVERY_BASES_PER_PARCEL_CENTRE = 8;
    public static final int MAX_NUMBER_OF_DELIVERY_BASES_PER_PARCEL_CENTRE = 10;
    public static final int MIN_NUMBER_OF_DELIVERY_AREAS_PER_DELIVERY_BASE = 5;
    public static final int MAX_NUMBER_OF_DELIVERY_AREAS_PER_DELIVERY_BASE = 10;
    public static final int MIN_NUMBER_OF_DELIVERY_SEGMENTS_PER_DELIVERY_AREA = 12;
    public static final int MAX_NUMBER_OF_DELIVERY_SEGMENTS_PER_DELIVERY_AREA = 20;
}
