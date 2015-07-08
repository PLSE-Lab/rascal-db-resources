package databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.List;

public class UnifiedBuilder {
	private String mysqlHost;
	private String mysqlDbName;
	private String mysqlUser;
	private String mysqlPassword;
	private String mongoHost;
	private String mongoDbName;
	private String mongoUser;
	private String mongoPassword;

	private boolean mysqlConfigured = false;
	private boolean mongoConfigured = false;

	private String tableName = "";
	private String documentName = "";
	
	private String q = "";
//	private String host = "";
//	private String dbName = "";
//	private String username = "";
//	private String password = "";
	
	private int columnsAdded = 0;
	
	private List<String> clauses = null;
	
	public final static String SELECT_ALL = "SELECT * ";
	public final static String SELECT_SPECIFIC = "SELECT ";
	// I don't think I'll use these/am allowed to use them with GHTorrent
	public final static String INSERT = "INSERT INTO "; 
	public final static String UPDATE_SPECIFIC = "UPDATE ";
	public final static String UPDATE_ALL = "UPDATE * ";
	public final static String DELETE = "DELETE ";

	private HashSet<String> mysqlColumnNames = new HashSet<String>();
	private Connection mysqlConnection;
	
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
		this.mongoHost = mongoHost;
		this.mongoDbName = mongoDbName;
		this.mongoUser = mongoUser;
		this.mongoPassword = mongoPassword;
		
		this.mongoConfigured = true;
		
		return this;
	}

	/*
	 * Columns get added in the reverse order they were called. Shouldn't be a problem though. 
	 */
	public MySQLQuery build(){
		//assemble string
		
		for(int i = 0; i < this.clauses.size(); i++){
			if(columnsAdded != 0 && i == 1 + columnsAdded){
				//take off the last ", "
				this.q = this.q.substring(0, this.q.length() - 2); 
				this.q += " ";
			}
			this.q += this.clauses.get(i);
		}
		this.q += ";";
		return new MySQLQuery(this.mysqlHost, this.mysqlDbName, this.mysqlUser, this.mysqlPassword, this.q);
	}
	public UnifiedBuilder setTable(String tableName){
		this.tableName = tableName;
		String from = "FROM " + this.mysqlDbName + "." + tableName;
		this.clauses.add(from);
		
		// TODO: Get the column names for the table
		
		return this; //is this the right thing to do?
	}
	/*
	 * Maybe have a String[] args ... (or however that works) to have an indefinite number of column names added with one method 
	 */
	public UnifiedBuilder getColumn(String columnName){
		columnsAdded++;
		this.clauses.add(1, columnName + ", ");
		return this;
	}
	/*
	 * to have an indefinite number of column names added with one method?
	 * Do I still need the one above this?
	 */
	public UnifiedBuilder getColumn(String ... args){
		columnsAdded += args.length;
		for(int i = 0; i < args.length; i++){
			this.clauses.add(1+i, args[i] + ", ");
		}
		return this;
	}
	
	public UnifiedBuilder where(String whereClause){
		this.clauses.add(" WHERE " + whereClause);
		return this; 
	}
}
