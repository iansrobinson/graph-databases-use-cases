package org.neo4j.graphdatabases.queries.testing;

public class IndexParam
{
    public static IndexParam indexParam( String nodeLabel, String propertyName )
    {
        return new IndexParam( nodeLabel, propertyName);
    }

    private final String nodeLabel;
    private final String propertyName;

    private IndexParam(String nodeLabel, String propertyName)
    {
        this.nodeLabel = nodeLabel;
        this.propertyName = propertyName;
    }

    public String nodeLabel()
    {
        return nodeLabel;
    }

    public String propertyName()
    {
        return propertyName;
    }
}
