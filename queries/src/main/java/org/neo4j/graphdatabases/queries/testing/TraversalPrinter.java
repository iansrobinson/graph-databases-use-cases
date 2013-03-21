package org.neo4j.graphdatabases.queries.testing;


import static java.lang.Math.min;
import static java.util.Arrays.asList;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class TraversalPrinter implements Evaluator
{
    private final Iterable<String> propertyNames;
    private final Evaluator innerEvaluator;
    private String lastPath = "";


    public TraversalPrinter( Evaluator innerEvaluator, String... propertyNames )
    {
        this.innerEvaluator = innerEvaluator;
        this.propertyNames = asList( propertyNames );
    }

    private void printPath( Path path )
    {
        StringBuilder builder = new StringBuilder();
        Node previousNode = null;
        for ( PropertyContainer propertyContainer : path )
        {
            if ( Node.class.isAssignableFrom( propertyContainer.getClass() ) )
            {
                previousNode = (Node) propertyContainer;
                builder.append( formatNode( previousNode ) );
            }
            else
            {
                builder.append( formatRelationship( (Relationship) propertyContainer, previousNode ) );
            }
        }
        String currentPath = builder.toString();


        String overlap = overlappingPath( currentPath, lastPath );
        if ( !overlap.isEmpty() )
        {
            String formattedCurrentPath = repeat( " ", overlap.length() ).concat( currentPath.substring( overlap
                    .length() ) );
            System.out.print( formattedCurrentPath );
        }
        else
        {
            System.out.print( currentPath );
        }


        lastPath = currentPath;

    }

    private String repeat( String s, int i )
    {
        return String.format( String.format( "%%0%dd", i ), 0 ).replace( "0", s );
    }

    private String overlappingPath( String s1, String s2 )
    {
        StringBuilder builder = new StringBuilder();
        int maxLength = min( s1.length(), s2.length() );
        for ( int i = 0; i < maxLength; i++ )
        {
            if ( s1.charAt( i ) == s2.charAt( i ) )
            {
                builder.append( s1.charAt( i ) );
            }
            else
            {
                break;
            }
        }
        String overlap = builder.toString();
        return overlap.substring( 0, overlap.lastIndexOf( ")" ) + 1 );
    }


    private Evaluation printResult( Evaluation evaluation )
    {
        System.out.print( String.format( " %s\n", evaluation.name() ) );
        return evaluation;
    }


    private String formatRelationship( Relationship relationship, Node previousNode )
    {
        String prefix = "";
        String suffix = "";

        if ( relationship.getStartNode().equals( previousNode ) )
        {
            suffix = ">";
        }
        else
        {
            prefix = "<";
        }

        return String.format( "%s-[:%s]-%s", prefix, relationship.getType().name(), suffix );
    }

    private String formatNode( Node node )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "(" );
        boolean propertyFound = false;
        Iterator<String> propertyNameIterator = propertyNames.iterator();
        while ( propertyNameIterator.hasNext() && !propertyFound )
        {
            String propertyName = propertyNameIterator.next();
            if ( node.hasProperty( propertyName ) )
            {
                builder.append( String.format( "%s:%s", propertyName, node.getProperty( propertyName ) ) );
                propertyFound = true;
            }
        }
        if ( !propertyFound )
        {
            Iterator<String> propertyKeyIterator = node.getPropertyKeys().iterator();
            if ( propertyKeyIterator.hasNext() )
            {
                String propertyName = propertyKeyIterator.next();
                builder.append( String.format( "%s:%s", propertyName, node.getProperty( propertyName ) ) );
            }
            else
            {
                builder.append( String.format( "id:%s", node.getId() ) );
            }
        }

        builder.append( ")" );
        return builder.toString();
    }

    public Evaluation evaluate( Path path )
    {
        printPath( path );
        return printResult( innerEvaluator.evaluate( path ) );
    }
}
