package databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class UnifiedBuilder {
	private String mysqlHost;
	private String mysqlDbName;
	private String mysqlUser;
	private String mysqlPassword;
	private String mongoHost;
	private String mongoDbName;
	private String mongoUser;
	private String mongoPassword;
	
	private MongoDatabase mongoDb;
	private MongoCollection<Document> mongoCollection;
	private Bson mongoFilter;
	private Bson mongoProjection;
	private MongoClient mongoClient;

	private boolean mysqlConfigured = false;
	private boolean mongoConfigured = false;

	private String tableName = "";
	private String documentName = "";
	
	private String query = "";
	
	private int columnsAdded = 0;
	private int mysqlFiltersAdded = 0;
	
	private List<String> mysqlClauses = new ArrayList<String>();
	private List<String> mysqlWhere = new ArrayList<String>();
	
	public final static String SELECT_ALL = "SELECT * ";
	public final static String SELECT_SPECIFIC = "SELECT ";
	// I don't think I'll use these/am allowed to use them with GHTorrent
	public final static String INSERT = "INSERT INTO "; 
	public final static String UPDATE_SPECIFIC = "UPDATE ";
	public final static String UPDATE_ALL = "UPDATE * ";
	public final static String DELETE = "DELETE ";

	private HashSet<String> mysqlColumnNames = new HashSet<String>();
	private HashSet<String> mongoProjectionFields = new HashSet<String>();
	private Connection mysqlConnection;
	private boolean includeID = false;
	private String mongoCollectionName;
	
	
	public UnifiedBuilder setMysqlConnectionInfo(String mysqlHost, String mysqlDbName, String mysqlUser, String mysqlPassword) {
		this.mysqlHost = mysqlHost;
		this.mysqlDbName = mysqlDbName;
		this.mysqlUser = mysqlUser;
		this.mysqlPassword = mysqlPassword;

		try {
			Class.forName("com.mysql.jdbc.Driver"); 
			this.mysqlConnection = DriverManager.getConnection("jdbc:mysql://" + this.mysqlHost + "/" + this.mysqlDbName 
					+ "?user=" + this.mysqlUser + "&password=" + this.mysqlPassword);
			this.mysqlConfigured = true;
		} catch(Exception e){
			e.printStackTrace();
		}

		return this;
	}

	public UnifiedBuilder setMongoConnectionInfo(String mongoHost, String mongoDbName, String mongoUser, String mongoPassword) {
		//I don't think I need these or the parameters(besides name)
		this.mongoHost = mongoHost;
		this.mongoDbName = mongoDbName;
		this.mongoUser = mongoUser;
		this.mongoPassword = mongoPassword;
		
		this.mongoClient = new MongoClient();
		this.mongoDb = this.mongoClient.getDatabase(this.mongoDbName);
		
		this.mongoConfigured = true;
		
		return this;
	}

	/*
	 * TODO: Change return type and what's returned 
	 * Still using this to check to make sure SQL statement is right
	 * 
	 * Maybe make a MySQLQuery and a MongoQuery member of the class that can be accessed outside? Add methods to retrieve those?
	 * 
	 */
	public MySQLQuery buildMySQL(){
		//assemble string
		this.query = SELECT_SPECIFIC;
		
		for(int i = 0; i < this.mysqlClauses.size(); i++){
			 
			if(columnsAdded != 0 && i == columnsAdded){
				//take off the last ", "
				this.query = this.query.substring(0, this.query.length() - 2); 
				 
				this.query += " ";
			}
			 
			this.query += this.mysqlClauses.get(i);
		}
		if(this.mysqlFiltersAdded > 0){
			this.query += " WHERE ";
		}
		for(int i = 0; i < this.mysqlWhere.size(); i++){
			
			this.query += this.mysqlWhere.get(i);
			if(this.mysqlFiltersAdded != 0 && i == this.mysqlFiltersAdded - 1){
				//take off the last ", "
				this.query += " ";
				this.query = this.query.substring(0, this.query.length() - 3); 
				
				
			}
			
		}
		this.query += ";";
		return new MySQLQuery(this.mysqlHost, this.mysqlDbName, this.mysqlUser, this.mysqlPassword, this.query);
	}
	/*
	 * TODO: Change return type and what's returned 
	 * Still using this to check to make sure Mongo statement is right
	 */
	public MongoDBQuery build(){
		return new MongoDBQuery(this.mongoDbName, this.mongoCollectionName, this.mongoProjectionFields , this.includeID , this.mongoFilter);
	}
	
	/*
	 * MySQL table name, also gets names of the columns in the specified table
	 */
	public UnifiedBuilder setTable(String tableName){
		this.tableName = tableName;
		String from = "FROM " + this.mysqlDbName + "." + tableName;
		this.mysqlClauses.add(from);
		
		// Get the column names for the table
		if(this.mysqlConfigured){
			
			ResultSet rs = null;
			ResultSetMetaData metaData = null;
			try {
				 //not sure if this would work or not...
				 Statement trial = this.mysqlConnection.createStatement();
				 //is there any way to get just one row to make it more efficient?
				 rs = trial.executeQuery("SELECT * from " + tableName + ";"); 
				 metaData = rs.getMetaData();
				 for(int i = 1; i <= metaData.getColumnCount(); i++){
					 this.mysqlColumnNames.add(metaData.getColumnName(i));
				 } 
				 
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return this;
	}
	/*
	 * sets the collection to use on the Mongo side
	 */
	public UnifiedBuilder setCollection(String collectionName){
		this.mongoCollectionName = collectionName;
		this.mongoCollection = this.mongoDb.getCollection(collectionName);
		return this;
	}
	/*
	 * Try to establish if the attribute desired is in the MySQL database or if its in Mongo
	 * Do I need to check if its in Mongo?
	 */
	public UnifiedBuilder getAttribute(String ... args){
		
		for(int i = 0; i < args.length; i++){
			if (this.mysqlColumnNames.contains(args[i])){
				this.mysqlClauses.add(this.columnsAdded, args[i] + ", ");
				this.columnsAdded++;
			} else {
				this.mongoProjectionFields.add(args[i]);
			}
		}
		
		return this;
	}
	//TODO: MySQL uses AND to separate WHERE clause stuff right?
	public UnifiedBuilder addSearchFilter(String field, Operator relation, String value){
		if(this.mysqlColumnNames.contains(field)){
			this.mysqlWhere.add(field + relation.toString() + value + ", ");
			this.mysqlFiltersAdded++;
		}else{
			//TODO: handle Mongo stuff
			Bson newFilter = null;
			switch(relation){
				case EQUAL_TO :{
					newFilter = eq(field, value);
					break;
				} 
				case LESS_THAN_OR_EQUAL_TO :{
					newFilter = lte(field, value);
					break;
				} 
				case GREATER_THAN_OR_EQUAL_TO :{
					newFilter = gte(field, value);
					break;
				} 
				case LESS_THAN : {
					newFilter = lt(field, value);
					break;
				} 
				case GREATER_THAN:{
					newFilter = gt(field, value);
					break;
				} case NOT_EQUAL_TO:{
					newFilter = ne(field, value);
					break;
				}
				default : {
					break;
				}
			}
			 
			if(this.mongoFilter != null){
				this.mongoFilter = and(this.mongoFilter, newFilter);
			} else {
				this.mongoFilter = newFilter;
			}
			 
		}
		return this;
	}
	
	/* 
	 * TODO: MySQL uses AND to separate WHERE clause stuff right?
	 * need to have because it wasn't working if I had a string of a number, 
	 * maybe be able to parse a string to have only one method?
	 */
		public UnifiedBuilder addSearchFilter(String field, Operator relation, float value){
			if(this.mysqlColumnNames.contains(field)){
				this.mysqlWhere.add(field + relation.toString() + value + ", ");
				this.mysqlFiltersAdded++;
			}else{
				//TODO: handle Mongo stuff
				Bson newFilter = null;
				switch(relation){
					case EQUAL_TO :{
						newFilter = eq(field, value);
						break;
					} 
					case LESS_THAN_OR_EQUAL_TO :{
						newFilter = lte(field, value);
						break;
					} 
					case GREATER_THAN_OR_EQUAL_TO :{
						newFilter = gte(field, value);
						break;
					} 
					case LESS_THAN : {
						newFilter = lt(field, value);
						break;
					} 
					case GREATER_THAN:{
						newFilter = gt(field, value);
						break;
					} case NOT_EQUAL_TO:{
						newFilter = ne(field, value);
						break;
					}
					default : {
						break;
					}
				}
				 
				if(this.mongoFilter != null){
					this.mongoFilter = and(this.mongoFilter, newFilter);
				} else {
					this.mongoFilter = newFilter;
				}
				 
			}
			return this;
		}
	
	/*
	 * MySQL method (replaced? by getAttribute)
	 */
	public UnifiedBuilder getColumn(String ... args){
		columnsAdded += args.length;
		for(int i = 0; i < args.length; i++){
			this.mysqlClauses.add(1+i, args[i] + ", ");
		}
		return this;
	}
	/*
	 * MySQL method (replace? with addSearchFilter?)
	 */
	public UnifiedBuilder where(String whereClause){
		this.mysqlClauses.add(" WHERE " + whereClause);
		return this; 
	}
}
