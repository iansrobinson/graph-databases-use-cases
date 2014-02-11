package org.neo4j.graphdatabases.queries;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;

public class AccessControlQueries
{
    private final ExecutionEngineWrapper executionEngine;

    public AccessControlQueries( ExecutionEngineWrapper executionEngineWrapper )
    {
        this.executionEngine = executionEngineWrapper;
    }

    public ExecutionResult findAccessibleResources( String adminName )
    {
        String query = "MATCH (admin:Administrator {name:{adminName}})\n" +
                "MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()\n" +
                "            <-[:CHILD_OF*0..3]-(company)<-[:WORKS_FOR]-(employee)\n" +
                "            -[:HAS_ACCOUNT]->(account)\n" +
                "WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company))\n" +
                "RETURN employee.name AS employee, account.name AS account\n" +
                "UNION\n" +
                "MATCH (admin:Administrator {name:{adminName}})\n" +
                "MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()\n" +
                "      <-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)\n" +
                "RETURN employee.name AS employee, account.name AS account";

        Map<String, Object> params = new HashMap<>();
        params.put( "adminName", adminName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult findAccessibleCompanies( String adminName )
    {
        String query = "MATCH (admin:Administrator {name:{adminName}})\n" +
                "MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company)\n" +
                "WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company))\n" +
                "RETURN company.name AS company\n" +
                "UNION\n" +
                "MATCH (admin:Administrator {name:{adminName}})\n" +
                "MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)\n" +
                "RETURN company.name AS company";

        Map<String, Object> params = new HashMap<>();
        params.put( "adminName", adminName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult findAccessibleAccountsForCompany( String adminName, String companyName )
    {
        String query =
                "MATCH (admin:Administrator {name:{adminName}}),\n" +
                        "      (company:Company {name:{companyName}})\n" +
                        "MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)\n" +
                        "      <-[:CHILD_OF*0..3]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)\n" +
                        "WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(subcompany))\n" +
                        "RETURN account.name AS account\n" +
                        "UNION\n" +
                        "MATCH (admin:Administrator {name:{adminName}}),\n" +
                        "      (company:Company {name:{companyName}})\n" +
                        "MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)\n" +
                        "      <-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)\n" +
                        "RETURN account.name AS account";

        Map<String, Object> params = new HashMap<>();
        params.put( "adminName", adminName );
        params.put( "companyName", companyName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult findAdminForResource( String resourceName )
    {
        String query = "MATCH (resource:Resource {name:{resourceName}})\n" +
                       "MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)\n" +
                       "        -[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin)\n" +
                       "WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company))\n" +
                       "RETURN admin.name AS admin\n" +
                       "UNION\n" +
                       "MATCH (resource:Resource {name:{resourceName}})\n" +
                       "MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)\n" +
                       "        <-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin)\n" +
                       "RETURN admin.name AS admin";

        Map<String, Object> params = new HashMap<>();
        params.put( "resourceName", resourceName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult findAdminForCompany( String companyName )
    {
        String query = "MATCH (company:Company {name:{companyName}})\n" +
                "MATCH (company)-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin)\n" +
                "WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company))\n" +
                "RETURN admin.name AS admin\n" +
                "UNION\n" +
                "MATCH (company:Company {name:{companyName}})\n" +
                "MATCH (company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin)\n" +
                "RETURN admin.name AS admin";

        Map<String, Object> params = new HashMap<>();
        params.put( "companyName", companyName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult hasAccessToResource( String adminName, String resourceName )
    {
        String query =
                "MATCH (admin:Administrator {name:{adminName}}),\n" +
                        "      (resource:Resource {name:{resourceName}})\n" +
                        "MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0." +
                        ".3]-(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource)\n" +
                        "WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company))\n" +
                        "RETURN count(p) AS accessCount\n" +
                        "UNION\n" +
                        "MATCH (admin:Administrator {name:{adminName}}),\n" +
                        "      (resource:Resource {name:{resourceName}})\n" +
                        "MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource)\n" +
                        "RETURN count(p) AS accessCount";

        Map<String, Object> params = new HashMap<>();
        params.put( "adminName", adminName );
        params.put( "resourceName", resourceName );

        return executionEngine.execute( query, params );
    }


    public ExecutionResult hasAccessToIndexedResource( String adminName, String resourceName )
    {
        String query =
                "MATCH (admin:Administrator {name:{adminName}}),\n" +
                        "      c1=(company)<-[:CHILD_OF*0..3]-(:Company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource:Resource {name:{resourceName}})\n" +
                        "MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)\n" +
                        "WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company))\n" +
                        "RETURN count(p) AS accessCount\n" +
//                        "RETURN p, company, admin, resource,c1\n" +
                        "UNION\n" +
                        "MATCH (admin:Administrator {name:{adminName}}),\n" +
                        "      c1=(company:Company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource:Resource {name:{resourceName}})\n" +
                        "MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)\n" +
                        "RETURN count(p) AS accessCount\n";
//                        "RETURN p, company, admin, resource,c1\n";

        Map<String, Object> params = new HashMap<>();
        params.put( "adminName", adminName );
        params.put( "resourceName", resourceName );

        return executionEngine.execute( query, params );
    }
}
