/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.graphdatabases.performance_tests.testing;

import java.util.Map;

public interface ParamsGenerator
{
    Map<String, String> generateParams();
}
