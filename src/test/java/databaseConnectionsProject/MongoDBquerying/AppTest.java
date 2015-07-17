package databaseConnectionsProject.MongoDBquerying;

import databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying.Operator;
import databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying.UnifiedBuilder;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
    
    /**
     * Get something that's only in Mongo 
     */
    public void test1(){
    	
    }
    
    /**
     * Get something that's only in MySQL 
     */
    public void test2(){
    	
    }
    
    /**
     * Get a little bit of both 
     */
    public void test3(){
    	String mysqlHost = "";
    	String mysqlDbName = "";
    	String mysqlUser = "";
    	String mysqlPassword = "";
    	String mongoHost = "";
    	String mongoDbName = "";
    	String mongoUser = "";
    	String mongoPassword = "";
    	UnifiedBuilder builder = new UnifiedBuilder();
    	builder.setMysqlConnectionInfo(mysqlHost, mysqlDbName, mysqlUser, mysqlPassword);
    	builder.setMongoConnectionInfo(mongoHost, mongoDbName, mongoUser, mongoPassword);
    	builder.setCollection("users");
    	builder.setTable("users");
    	builder.getAttribute("name", "email", "followers"); 
    	builder.addNumericalFilter("id", Operator.EQUAL_TO, 1, true);
    }
    
    
    
    
}
