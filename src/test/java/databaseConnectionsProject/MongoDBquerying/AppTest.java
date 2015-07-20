package databaseConnectionsProject.MongoDBquerying;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

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
     * Get public repos, public gists, followers, and following from Mongo users 
     */
    public void test1(){
    	//TODO: Set connection info for GHTorrent
    	String mongoHost = "";
    	String mongoDbName = "";
    	String mongoUser = "";
    	String mongoPassword = "";

    	List<Document> actualResults = null;
    	List<Document> expectedResults = null;
    	
    	//Using Unified Builder
    	UnifiedBuilder builder = new UnifiedBuilder();
    	builder.setMongoConnectionInfo(mongoHost, mongoDbName, mongoUser, mongoPassword);
    	builder.setCollection("users");
    	builder.getAttribute("public_repos", "public_gists", "followers", "following"); 
    	builder.addNumericalFilter("id", Operator.EQUAL_TO, 1, true); //might only work on mysql part as the code is right now
    	builder.build();
    	
    	//get results from Unified Builder
    	try {
			builder.execute();
			actualResults = builder.getMongo();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	//get mongo results standard way
    	MongoClient mongoClient = new MongoClient();
		MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
		MongoCollection<Document> coll = mongoDb.getCollection("users");
		Bson filter = eq("id", 1);
		Bson projection = fields(include("public_repos", "public_gists", "followers", "following"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	mongoClient.close();
    	//The parameters may be in reverse order 
    	assertEquals(actualResults, expectedResults); 
    }
    
    /**
     * Gets name, email company, and location from MySQL users
     */
    public void test2(){
    	//TODO: Set connection info for GHTorrent
    	String mysqlHost = "";
    	String mysqlDbName = "";
    	String mysqlUser = "";
    	String mysqlPassword = "";
    	
    	ResultSet actualrs = null;
    	ResultSet expectedrs = null;
    	
    	//Using Unified Builder
    	UnifiedBuilder builder = new UnifiedBuilder();
    	builder.setMysqlConnectionInfo(mysqlHost, mysqlDbName, mysqlUser, mysqlPassword);
    	builder.setTable("users");
    	builder.getAttribute("name", "email", "company", "location"); 
    	builder.addNumericalFilter("id", Operator.EQUAL_TO, 1, true); //might only work on mysql part as the code is right now
    	builder.build();
    	
    	//get results from Unified Builder
    	try {
			builder.execute();
			actualrs = builder.getMySQL();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	//get mysql results standard way
    	try {
			Class.forName("com.mysql.jdbc.Driver"); 
			Connection c = DriverManager.getConnection("jdbc:mysql://" + mysqlHost + "/" + mysqlDbName 
					+ "?user=" + mysqlUser + "&password=" + mysqlPassword);
			Statement stmt = c.createStatement();
			String query = "SELECT name, email, company, location FROM users WHERE id=1"; //should be right
			expectedrs = stmt.executeQuery(query);
			
		} catch(Exception e){
			e.printStackTrace();
		}
    	
    	//The parameters may be in reverse order 
    	assertEquals(actualrs, expectedrs); 
    }
    
    /**
     * Get a little bit of both 
     * In users, gets name and email from mysql, followers from mongo
     */
    public void test3(){
    	//TODO: Set connection info for GHTorrent
    	String mysqlHost = "";
    	String mysqlDbName = "";
    	String mysqlUser = "";
    	String mysqlPassword = "";
    	String mongoHost = "";
    	String mongoDbName = "";
    	String mongoUser = "";
    	String mongoPassword = "";
    	
    	ResultSet actualrs = null;
    	ResultSet expectedrs = null;
    	List<Document> actualResults = null;
    	List<Document> expectedResults = null;
    	
    	//Using Unified Builder
    	UnifiedBuilder builder = new UnifiedBuilder();
    	builder.setMysqlConnectionInfo(mysqlHost, mysqlDbName, mysqlUser, mysqlPassword);
    	builder.setMongoConnectionInfo(mongoHost, mongoDbName, mongoUser, mongoPassword);
    	builder.setCollection("users");
    	builder.setTable("users");
    	builder.getAttribute("name", "email", "followers"); 
    	builder.addNumericalFilter("id", Operator.EQUAL_TO, 1, true); //might only work on mysql part as the code is right now
    	builder.build();
    	
    	//get results from Unified Builder
    	try {
			builder.execute();
			actualrs = builder.getMySQL();
			actualResults = builder.getMongo();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	//get mysql results standard way
    	try {
			Class.forName("com.mysql.jdbc.Driver"); 
			Connection c = DriverManager.getConnection("jdbc:mysql://" + mysqlHost + "/" + mysqlDbName 
					+ "?user=" + mysqlUser + "&password=" + mysqlPassword);
			Statement stmt = c.createStatement();
			String query = "SELECT name, email FROM users WHERE id=1"; //should be right
			expectedrs = stmt.executeQuery(query);
			
		} catch(Exception e){
			e.printStackTrace();
		}
    	//get mongo results standard way
    	MongoClient mongoClient = new MongoClient();
		MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
		MongoCollection<Document> coll = mongoDb.getCollection("users");
		Bson filter = eq("id", 1);
		Bson projection = fields(include("followers"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	
    	//The parameters may be in reverse order 
    	assertEquals(actualrs, expectedrs);
    	assertEquals(actualResults, expectedResults); 
    	
    }
    
    /*
     * TODO:
     * 		Make similar test cases where I get more than one thing back from each (use < and >)
     * 		Complex filter? (use both AND and OR)
     * 		Other databases?
     * 		Should I be using the MySQL as feedback to search Mongo more efficiently?
     * 
     * 	These might be too much:
     * 		one with no filter, one with no projection, one to get everything(no parameters)
     */
    
    
    
}
