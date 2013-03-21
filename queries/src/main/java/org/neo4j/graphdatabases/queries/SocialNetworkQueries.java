package org.neo4j.graphdatabases.queries;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class SocialNetworkQueries
{
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
                "START  subject=node:user(name={name})\n" +
                        "MATCH  subject-[:WORKS_FOR]->company<-[:WORKS_FOR]-person,\n" +
                        "       subject-[:INTERESTED_IN]->interest<-[:INTERESTED_IN]-person\n" +
                        "RETURN person.name AS name,\n" +
                        "       COUNT(interest) AS score,\n" +
                        "       COLLECT(interest.name) AS interests\n" +
                        "ORDER BY score DESC";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult sharedInterestsAllCompanies( String userName, int limit )
    {
        String query =
                "START  subject=node:user(name={name})\n" +
                        "MATCH  subject-[:INTERESTED_IN]->interest<-[:INTERESTED_IN]-person,\n" +
                        "       person-[:WORKS_FOR]->company\n" +
                        "RETURN person.name AS name,\n" +
                        "       company.name AS company,\n" +
                        "       COUNT(interest) AS score,\n" +
                        "       COLLECT(interest.name) AS interests\n" +
                        "ORDER BY score DESC";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );
        params.put( "resultLimit", limit );

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult sharedInterestsAlsoInterestedInTopic( String userName, String topicLabel )
    {
        String query = "START person=node:user(name={name})\n" +
                "MATCH person-[:INTERESTED_IN]->()<-[:INTERESTED_IN]-colleague-[:INTERESTED_IN]->topic\n" +
                "WHERE topic.name={topic}\n" +
                "WITH  colleague\n" +
                "MATCH colleague-[:INTERESTED_IN]->allTopics\n" +
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
                "START subject=node:user(name={name})\n" +
                        "MATCH p=subject-[:WORKED_ON]->()-[:WORKED_ON*0..2]-()\n" +
                        "        <-[:WORKED_ON]-person-[:INTERESTED_IN]->interest\n" +
                        "WHERE person<>subject AND interest.name={topic}\n" +
                        "WITH DISTINCT person.name AS name,\n" +
                        "     MIN(LENGTH(p)) as pathLength\n" +
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
        Node user = db.index().forNodes( "user" ).get( "name", userName ).getSingle();
        final Node topic = db.index().forNodes( "topic" ).get( "name", topicLabel ).getSingle();

        final RelationshipType interested_in = withName( "INTERESTED_IN" );
        final RelationshipType worked_on = withName( "WORKED_ON" );

        TraversalDescription traversalDescription = Traversal.description()
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
                "START subject=node:user(name={name})\n" +
                        "MATCH p=subject-[:WORKED_ON]->()-[:WORKED_ON*0..2]-()\n" +
                        "        <-[:WORKED_ON]-person-[:INTERESTED_IN]->interest\n" +
                        "WHERE person<>subject AND interest.name IN {interests}\n" +
                        "WITH person, interest, MIN(LENGTH(p)) as pathLength\n" +
                        "RETURN person.name AS name,\n" +
                        "       COUNT(interest) AS score,\n" +
                        "       COLLECT(interest.name) AS interests,\n" +
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
        String query = "START subject=node:user(name={name})\n" +
                "MATCH p=subject-[:WORKED_WITH*0..1]-()-[:WORKED_WITH]-person-[:INTERESTED_IN]->interest\n" +
                "WHERE person<>subject AND interest.name IN {interests}\n" +
                "WITH person, interest, MIN(LENGTH(p)) as pathLength\n" +
                "RETURN person.name AS name,\n" +
                "       COUNT(interest) AS score,\n" +
                "       COLLECT(interest.name) AS interests,\n" +
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

    public ExecutionResult createWorkedWithRelationships( String userName )
    {

        String query = "START subject = node:user(name={name})\n" +
                "MATCH subject-[:WORKED_ON]->()<-[:WORKED_ON]-person\n" +
                "WHERE NOT(subject-[:WORKED_WITH]-person)\n" +
                "WITH DISTINCT subject, person\n" +
                "CREATE UNIQUE subject-[:WORKED_WITH]-person\n" +
                "RETURN subject.name AS startName, person.name AS endName";


        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", userName );

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult getAllUsers()
    {
        String query = "START subject = node:user('name:*')\n" +
                "RETURN subject.name AS name";

        return executionEngineWrapper.execute( query, new HashMap<String, Object>() );
    }


//    public GremlinPipeline friendOfAFriendWithParticularInterestGremlin( String userName,
//                                                                         final String topicLabel )
//            throws ScriptException
//    {
//        Map<String, String> params = new HashMap<String, String>();
//        params.put( "userName", userName );
//        params.put( "topicLabel", topicLabel );
//
////        String script = "g.V('name', userName).out('WORKED_ON').in('WORKED_ON').loop(2){it.loops < 4}.dedup().as
//// ('v')" +
////                ".out('INTERESTED_IN').filter{it.name==topicLabel}.back('v').name";
//        String script = "g.V('name', userName).out('WORKED_ON').in('WORKED_ON').loop(2){it.loops < 3}.and(_().out" +
//                "('INTERESTED_IN').filter{it.name==topicLabel}).dedup().name";
//
//        ScriptEngine engine = new GremlinGroovyScriptEngine();
//
//        final Bindings bindings = createBindings( db, params );
//
//        return (GremlinPipeline) engine.eval( script, bindings );
//    }

//    private Bindings createBindings( GraphDatabaseService neo4j, Map params )
//    {
//        final Bindings bindings = createInitialBinding( neo4j );
//        if ( params != null )
//        {
//            bindings.putAll( params );
//        }
//        return bindings;
//    }
//
//    private Bindings createInitialBinding( GraphDatabaseService neo4j )
//    {
//        final Bindings bindings = new SimpleBindings();
//        final Neo4jGraph graph = new Neo4jGraph( neo4j, false );
//        bindings.put( "g", graph );
//        return bindings;
//    }
}
