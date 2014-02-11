package org.neo4j.graphdatabases.queries;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.collection.IteratorUtil;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class SocialNetworkQueries
{
    public static final Label USER = DynamicLabel.label("User");
    public static final Label TOPIC = DynamicLabel.label("Topic");
    private final GraphDatabaseService db;
    private final ExecutionEngineWrapper executionEngineWrapper;


    public SocialNetworkQueries( GraphDatabaseService db, ExecutionEngineWrapper executionEngineWrapper )
    {
        this.db = db;
        this.executionEngineWrapper = executionEngineWrapper;
    }

    public ExecutionResult sharedInterestsSameCompany( String userName )
    {
        String query =
                "MATCH  (subject:User {name:{name}})\n" +
                        "MATCH  (subject)-[:WORKS_FOR]->(company)<-[:WORKS_FOR]-(person),\n" +
                        "       (subject)-[:INTERESTED_IN]->(interest)<-[:INTERESTED_IN]-(person)\n" +
                        "RETURN person.name AS name,\n" +
                        "       count(interest) AS score,\n" +
                        "       collect(interest.name) AS interests\n" +
                        "ORDER BY score DESC";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult sharedInterestsAllCompanies( String userName, int limit )
    {
        String query =
                "MATCH  (subject:User {name:{name}})\n" +
                        "MATCH  (subject)-[:INTERESTED_IN]->(interest)<-[:INTERESTED_IN]-(person),\n" +
                        "       (person)-[:WORKS_FOR]->(company)\n" +
                        "RETURN person.name AS name,\n" +
                        "       company.name AS company,\n" +
                        "       count(interest) AS score,\n" +
                        "       collect(interest.name) AS interests\n" +
                        "ORDER BY score DESC";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );
        params.put( "resultLimit", limit );

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult sharedInterestsAlsoInterestedInTopic( String userName, String topicLabel )
    {
        String query = "MATCH (person:User {name:{name}})\n" +
                "MATCH (person)-[:INTERESTED_IN]->()<-[:INTERESTED_IN]-(colleague)-[:INTERESTED_IN]->(topic)\n" +
                "WHERE topic.name={topic}\n" +
                "WITH  colleague\n" +
                "MATCH (colleague)-[:INTERESTED_IN]->(allTopics)\n" +
                "RETURN colleague.name AS name, collect(distinct(allTopics.name)) AS topics";


        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );
        params.put( "topicQuery", "name:" + topicLabel );
        params.put( "topic", topicLabel );

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult friendOfAFriendWithInterest( String userName, String topicLabel, int limit )
    {
        String query =
                "MATCH (subject:User {name:{name}})\n" +
                        "MATCH p=(subject)-[:WORKED_ON]->()-[:WORKED_ON*0..2]-()\n" +
                        "        <-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)\n" +
                        "WHERE person<>subject AND interest.name={topic}\n" +
                        "WITH DISTINCT person.name AS name,\n" +
                        "     min(length(p)) as pathLength\n" +
                        "ORDER BY pathLength ASC\n" +
                        "LIMIT {resultLimit}\n" +
                        "RETURN name, pathLength";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );
        params.put( "topicQuery", "name:" + topicLabel );
        params.put( "topic", topicLabel );
        params.put( "resultLimit", limit );

        return executionEngineWrapper.execute( query, params );
    }

    public Collection<Node> friendOfAFriendWithInterestTraversalFramework( String userName,
                                                                           final String topicLabel,
                                                                           int limit )
    {
        Node user = IteratorUtil.single(db.findNodesByLabelAndProperty(USER, "name", userName));
        final Node topic = IteratorUtil.single(db.findNodesByLabelAndProperty(TOPIC, "name", topicLabel));

        final RelationshipType interested_in = withName( "INTERESTED_IN" );
        final RelationshipType worked_on = withName( "WORKED_ON" );

        TraversalDescription traversalDescription = db.traversalDescription()
                .breadthFirst()
                .uniqueness( Uniqueness.NODE_GLOBAL )
                .relationships( worked_on )
                .evaluator( new
                            Evaluator()
                            {
                                @Override
                                public Evaluation evaluate( Path path )
                                {
                                    if ( path.length() == 0 )
                                    {
                                        return Evaluation.EXCLUDE_AND_CONTINUE;
                                    }

                                    Node currentNode = path.endNode();


                                    if ( path.length() % 2 == 0 )
                                    {
                                        for ( Relationship rel : currentNode.getRelationships(
                                                interested_in,
                                                Direction.OUTGOING ) )
                                        {
                                            if ( rel.getEndNode().equals( topic ) )
                                            {
                                                if ( path.length() % 4 == 0 )
                                                {
                                                    return Evaluation.INCLUDE_AND_PRUNE;
                                                }
                                                else
                                                {
                                                    return Evaluation.INCLUDE_AND_CONTINUE;
                                                }

                                            }
                                        }
                                    }

                                    if ( path.length() % 4 == 0 )
                                    {
                                        return Evaluation.EXCLUDE_AND_PRUNE;
                                    }
                                    else
                                    {
                                        return Evaluation.EXCLUDE_AND_CONTINUE;
                                    }
                                }
                            } );

        Iterable<Node> nodes = traversalDescription.traverse( user ).nodes();


        Iterator<Node> iterator = nodes.iterator();
        int nodeCount = 0;
        List<Node> results = new ArrayList<Node>();

        while ( iterator.hasNext() && nodeCount++ < limit )
        {
            results.add( iterator.next() );
        }

        return results;
    }

    public ExecutionResult friendOfAFriendWithMultipleInterest( String userName, int limit, String... interestLabels )
    {
        String query =
                "MATCH (subject:User {name:{name}})\n" +
                        "MATCH p=(subject)-[:WORKED_ON]->()-[:WORKED_ON*0..2]-()\n" +
                        "        <-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)\n" +
                        "WHERE person<>subject AND interest.name IN {interests}\n" +
                        "WITH person, interest, min(length(p)) as pathLength\n" +
                        "ORDER BY interest.name\n"+
                        "RETURN person.name AS name,\n" +
                        "       count(interest) AS score,\n" +
                        "       collect(interest.name) AS interests,\n" +
                        "       ((pathLength - 1)/2) AS distance\n" +
                        "ORDER BY score DESC\n" +
                        "LIMIT {resultLimit}";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );
        params.put( "interests", interestLabels );
        params.put( "resultLimit", limit );

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult friendWorkedWithFriendWithInterests( String userName, int limit, String... interestLabels )
    {
        String query = "MATCH (subject:User {name:{name}})\n" +
                "MATCH p=(subject)-[:WORKED_WITH*0..1]-()-[:WORKED_WITH]-(person)\n" +
                "        -[:INTERESTED_IN]->(interest)\n" +
                "WHERE person<>subject AND interest.name IN {interests}\n" +
                "WITH person, interest, min(length(p)) as pathLength\n" +
                "RETURN person.name AS name,\n" +
                "       count(interest) AS score,\n" +
                "       collect(interest.name) AS interests,\n" +
                "       (pathLength - 1) AS distance\n" +
                "ORDER BY score DESC\n" +
                "LIMIT {resultLimit}";


        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );
        params.put( "userQuery", "name:" + userName );
        params.put( "interests", interestLabels );
        params.put( "resultLimit", limit );

        StringBuilder builder = new StringBuilder();
        builder.append( "[" );
        for ( int i = 0; i < interestLabels.length; i++ )
        {
            builder.append( "'" );
            builder.append( interestLabels[i] );
            builder.append( "'" );
            if ( i < interestLabels.length - 1 )
            {
                builder.append( "," );
            }
        }
        builder.append( "]" );
        params.put( "topicQuery", builder.toString() );

        return executionEngineWrapper.execute( query, params );
    }

    // todo no result?
    public ExecutionResult createWorkedWithRelationships( String userName )
    {

        String query = "MATCH (subject:User {name:{name}})\n" +
                "MATCH (subject)-[:WORKED_ON]->()<-[:WORKED_ON]-(person)\n" +
                "WHERE NOT((subject)-[:WORKED_WITH]-(person))\n" +
                "WITH DISTINCT subject, person\n" +
                "CREATE UNIQUE (subject)-[:WORKED_WITH]-(person)\n" +
                "RETURN subject.name AS startName, person.name AS endName";


        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult getAllUsers()
    {
        String query = "MATCH (subject:User)\n" +
                "RETURN subject.name AS name";

        return executionEngineWrapper.execute( query, new HashMap<String, Object>() );
    }
}
