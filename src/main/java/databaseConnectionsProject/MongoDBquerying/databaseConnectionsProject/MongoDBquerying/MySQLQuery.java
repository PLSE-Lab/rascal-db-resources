package databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MySQLQuery {
	
	private String query = "";
	private String db = "";
	private String table = "";
	private Connection c = null;
	private Statement stmt = null;
	
	MySQLQuery(String host, String dbName, String username, String password, String query){
		this.query = query;
		this.db = dbName;
		try {
			Class.forName("com.mysql.jdbc.Driver"); 
			this.c = DriverManager.getConnection("jdbc:mysql://" + host + "/" + dbName 
					+ "?user=" + username + "&password=" + password);
			stmt = c.createStatement();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public String toString(){
		
		return this.query;
	}
	
	public ResultSet execute() throws SQLException{
		return stmt.executeQuery(this.query);
		
	}
	
	public static class Builder {
		private String q = "";
		private String host = "";
		private String tableName = "";
		private String dbName = "";
		private String username = "";
		private String password = "";
		
		private int columnsAdded = 0;
		
		private List<String> clauses = null;
		
		public final static String SELECT_ALL = "SELECT * ";
		public final static String SELECT_SPECIFIC = "SELECT ";
		// I don't think I'll use these/am allowed to use them with GHTorrent
		public final static String INSERT = "INSERT INTO "; 
		public final static String UPDATE_SPECIFIC = "UPDATE ";
		public final static String UPDATE_ALL = "UPDATE * ";
		public final static String DELETE = "DELETE ";
		
		Builder(String type, String host, String dbName, String username, String password){
			this.host = host;
			this.dbName = dbName;
			this.username = username;
			this.password = password;
			
			this.clauses = new ArrayList<String>();
			this.clauses.add(type);
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
			return new MySQLQuery(this.host, this.dbName, this.username, this.password, this.q);
		}
		public Builder setTable(String tableName){
			this.tableName = tableName;
			String from = "FROM " + this.dbName + "." + tableName;
			this.clauses.add(from);
			return this; //is this the right thing to do?
		}
		/*
		 * Maybe have a String[] args ... (or however that works) to have an indefinite number of column names added with one method 
		 */
		public Builder getColumn(String columnName){
			columnsAdded++;
			this.clauses.add(1, columnName + ", ");
			return this;
		}
		/*
		 * to have an indefinite number of column names added with one method?
		 * Do I still need the one above this?
		 */
		public Builder getColumn(String ... args){
			columnsAdded += args.length;
			for(int i = 0; i < args.length; i++){
				this.clauses.add(1+i, args[i] + ", ");
			}
			return this;
		}
		
		public Builder where(String whereClause){
			this.clauses.add(" WHERE " + whereClause);
			return this; 
		}
	}
}
