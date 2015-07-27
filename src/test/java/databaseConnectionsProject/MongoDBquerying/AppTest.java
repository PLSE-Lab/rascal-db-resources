package databaseConnectionsProject.MongoDBquerying;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
    	String mongoHost = "";
    	String mongoDbName = "github";
    	String mongoUser = "";
    	String mongoPassword = "";

    	List<Document> actualResults = null;
    	List<Document> expectedResults = null;
    	
    	//Using Unified Builder
    	UnifiedBuilder builder = new UnifiedBuilder();
    	builder.setMongoConnectionInfo(mongoHost, mongoDbName, mongoUser, mongoPassword);
    	builder.setCollection("users");
    	builder.getAttribute("public_repos", "public_gists", "followers", "following"); 
    	builder.addNumericalFilter("id", Operator.EQUAL_TO, 738779, true); //might only work on mysql part as the code is right now
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
		Bson filter = eq("id", 738779);
		Bson projection = fields(include("public_repos", "public_gists", "followers", "following"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	mongoClient.close();
    	//The parameters may be in reverse order 
    	assertEquals(expectedResults, actualResults); 
    	assertTrue(actualResults != null);
    }
    
    /**
     * Gets name, email company, and location from MySQL users
     */
    public void test2(){
    	String mysqlHost = "localhost:3306";
    	String mysqlDbName = "ghtorrent";
    	String mysqlUser = "java";
    	String mysqlPassword = "password";
    	
    	ResultSet actualrs = null;
    	ResultSet expectedrs = null;
    	
    	//Using Unified Builder
    	UnifiedBuilder builder = new UnifiedBuilder();
    	builder = builder.setMysqlConnectionInfo(mysqlHost, mysqlDbName, mysqlUser, mysqlPassword);
    	builder = builder.setTable("users");
    	builder = builder.getAttribute("name", "email", "company", "location");
    	builder = builder.addNumericalFilter("id", Operator.EQUAL_TO, 1, true); //might only work on mysql part as the code is right now
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
    	//assertEquals(actualrs, expectedrs); 
    	
    	try {
			while(actualrs.next() && expectedrs.next()){
				for(int i = 1; i <= 4; i++){
					assertEquals(expectedrs.getString(i), actualrs.getString(i));
				}
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	assertTrue(actualrs != null);
    }
    
    /**
     * Get a little bit of both 
     * In users, gets name and email from mysql, followers from mongo
     */

    public void test3(){
    	
    	String mysqlHost = "localhost";
    	String mysqlDbName = "ghtorrent";
    	String mysqlUser = "java";
    	String mysqlPassword = "password";
    	String mongoHost = "";
    	String mongoDbName = "github";
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
    	builder.addNumericalFilter("id", Operator.EQUAL_TO, 738779, true); //might only work on mysql part as the code is right now
    	//NOTE: I think ^ is fixed, but leaving comment just in case its not
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
			String query = "SELECT name, email FROM users WHERE id=738779"; //should be right
			expectedrs = stmt.executeQuery(query);
			
		} catch(Exception e){
			e.printStackTrace();
		}
    	//get mongo results standard way
    	MongoClient mongoClient = new MongoClient();
		MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
		MongoCollection<Document> coll = mongoDb.getCollection("users");
		Bson filter = eq("id", 738779);
		Bson projection = fields(include("followers"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	
    	//The parameters may be in reverse order 
		try {
			while(actualrs.next() && expectedrs.next()){
				for(int i = 1; i <= 2; i++){
					assertEquals(expectedrs.getString(i), actualrs.getString(i));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
    	assertEquals(expectedResults, actualResults); 
    	assertTrue(actualrs != null);
    	assertTrue(actualResults != null);
    	
    }
    /**
     * get more than one thing from both (uses <)
     */
    public void test4(){
    	String mysqlHost = "localhost";
    	String mysqlDbName = "ghtorrent";
    	String mysqlUser = "java";
    	String mysqlPassword = "password";
    	String mongoHost = "";
    	String mongoDbName = "github";
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
    	builder.addNumericalFilter("id", Operator.LESS_THAN, 1000, true); //might only work on mysql part as the code is right now
    	//NOTE: I think ^ is fixed, but leaving comment just in case its not
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
			String query = "SELECT name, email FROM users WHERE id<1000"; //should be right
			expectedrs = stmt.executeQuery(query);
			
		} catch(Exception e){
			e.printStackTrace();
		}
    	//get mongo results standard way
    	MongoClient mongoClient = new MongoClient();
		MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
		MongoCollection<Document> coll = mongoDb.getCollection("users");
		Bson filter = lt("id", 1000);
		Bson projection = fields(include("followers"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	
    	//The parameters may be in reverse order 
		try {
			while(actualrs.next() && expectedrs.next()){
				for(int i = 1; i <= 2; i++){
					assertEquals(expectedrs.getString(i), actualrs.getString(i));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
    	assertEquals(expectedResults, actualResults); 
    	assertTrue(actualrs != null);
    	assertTrue(actualResults != null);
    	
    }
    /**
     * get more than one thing from both (uses >)
     */
    public void test5(){
    	String mysqlHost = "localhost";
    	String mysqlDbName = "ghtorrent";
    	String mysqlUser = "java";
    	String mysqlPassword = "password";
    	String mongoHost = "";
    	String mongoDbName = "github";
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
    	builder.addNumericalFilter("id", Operator.GREATER_THAN, 11710360, true); //might only work on mysql part as the code is right now
    	//NOTE: I think ^ is fixed, but leaving comment just in case its not
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
			String query = "SELECT name, email FROM users WHERE id>11710360"; //should be right
			expectedrs = stmt.executeQuery(query);
			
		} catch(Exception e){
			e.printStackTrace();
		}
    	//get mongo results standard way
    	MongoClient mongoClient = new MongoClient();
		MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
		MongoCollection<Document> coll = mongoDb.getCollection("users");
		Bson filter = gt("id", 11710360);
		Bson projection = fields(include("followers"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	
    	//The parameters may be in reverse order 
		try {
			while(expectedrs.next() && actualrs.next()){
				for(int i = 1; i <= 2; i++){
					assertEquals(expectedrs.getString(i), actualrs.getString(i));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
    	assertEquals(expectedResults, actualResults); 
    	assertTrue(actualrs != null);
    	assertTrue(actualResults != null);
    	
    }
    /**
     * Use AND to get a range of values 
     */
    public void test6(){
    	String mysqlHost = "localhost";
    	String mysqlDbName = "ghtorrent";
    	String mysqlUser = "java";
    	String mysqlPassword = "password";
    	String mongoHost = "";
    	String mongoDbName = "github";
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
    	builder.addNumericalFilter("id", Operator.GREATER_THAN, 738000, true);
    	builder.addNumericalFilter("id", Operator.LESS_THAN, 739000, true);
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
			String query = "SELECT name, email FROM users WHERE id<739000 AND id>738000"; //should be right
			expectedrs = stmt.executeQuery(query);
			
		} catch(Exception e){
			e.printStackTrace();
		}
    	//get mongo results standard way
    	MongoClient mongoClient = new MongoClient();
		MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
		MongoCollection<Document> coll = mongoDb.getCollection("users");
		Bson filter = and(gt("id", 738000), lt("id", 739000));
		Bson projection = fields(include("followers"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	
    	//The parameters may be in reverse order 
		try {
			while(expectedrs.next() && actualrs.next()){
				for(int i = 1; i <= 2; i++){
					assertEquals(expectedrs.getString(i), actualrs.getString(i));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
    	assertEquals(expectedResults, actualResults); 
    	assertTrue(actualrs != null);
    	assertTrue(actualResults != null);
    	
    }
    
    /**
     * Use OR to get a range of values 
     */
    public void test7(){
    	String mysqlHost = "localhost";
    	String mysqlDbName = "ghtorrent";
    	String mysqlUser = "java";
    	String mysqlPassword = "password";
    	String mongoHost = "";
    	String mongoDbName = "github";
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
    	builder.addNumericalFilter("id", Operator.GREATER_THAN, 11710360, false);
    	builder.addNumericalFilter("id", Operator.LESS_THAN, 1000, false);
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
			String query = "SELECT name, email FROM users WHERE id>11710360 OR id<1000"; //should be right
			expectedrs = stmt.executeQuery(query);
			
		} catch(Exception e){
			e.printStackTrace();
		}
    	//get mongo results standard way
    	MongoClient mongoClient = new MongoClient();
		MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
		MongoCollection<Document> coll = mongoDb.getCollection("users");
		Bson filter = or(gt("id", 11710360), lt("id", 1000));
		Bson projection = fields(include("followers"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	
    	//The parameters may be in reverse order 
		try {
			while(expectedrs.next() && actualrs.next()){
				for(int i = 1; i <= 2; i++){
					assertEquals(expectedrs.getString(i), actualrs.getString(i));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
    	assertEquals(expectedResults, actualResults); 
    	assertTrue(actualrs != null);
    	assertTrue(actualResults != null);
    	
    }
    
    /**
     * Use both AND and OR to get a range of values 
     */
    public void test8(){
    	String mysqlHost = "localhost";
    	String mysqlDbName = "ghtorrent";
    	String mysqlUser = "java";
    	String mysqlPassword = "password";
    	String mongoHost = "";
    	String mongoDbName = "github";
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
    	builder.addNumericalFilter("id", Operator.LESS_THAN, 739000, true);
    	builder.addNumericalFilter("id", Operator.GREATER_THAN, 738000, true);
    	builder.addNumericalFilter("id", Operator.GREATER_THAN, 11710360, false);
    	builder.addNumericalFilter("id", Operator.LESS_THAN, 1000, false);
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
			String query = "SELECT name, email FROM users WHERE id<739000 AND id>738000 OR id>11710360 OR id<1000"; //should be right
			expectedrs = stmt.executeQuery(query);
			
		} catch(Exception e){
			e.printStackTrace();
		}
    	//get mongo results standard way
    	MongoClient mongoClient = new MongoClient();
		MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
		MongoCollection<Document> coll = mongoDb.getCollection("users");
		Bson filter = or(gt("id", 11710360), lt("id", 1000), and(gt("id", 738000), lt("id", 739000)));
		Bson projection = fields(include("followers"), excludeId()); 
		expectedResults = coll.find(filter).projection(projection).into(new ArrayList<Document>());
    	
    	//The parameters may be in reverse order 
		try {
			while(expectedrs.next() && actualrs.next()){
				for(int i = 1; i <= 2; i++){
					assertEquals(expectedrs.getString(i), actualrs.getString(i));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
    	assertEquals(expectedResults, actualResults); 
    	assertTrue(actualrs != null);
    	assertTrue(actualResults != null);
    	
    }
    
    /*
     * TODO:
     * 
     * 	These might be too much:
     * 		one with no filter, one with no projection, one to get everything(no parameters)
     */
    
    
    
}
