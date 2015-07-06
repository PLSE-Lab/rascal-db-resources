package databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

public class MongoDBQuery {
	
	//add static String constants to define the relational operators
	public static final String EQUAL_TO = "=";
	public static final String NOT_EQUAL_TO = "!=";
	public static final String LESS_THAN = "<";
	public static final String LESS_THAN_OR_EQUAL_TO = "<=";
	public static final String GREATER_THAN = ">";
	public static final String GREATER_THAN_OR_EQUAL_TO = ">=";
	
	private MongoDatabase db;
	private MongoCollection<Document> c;
	private Bson filter;
	private Bson projection;
	private MongoClient client;
	
	
	
	MongoDBQuery(String dbName, String collectionName, String[] projectionFields, boolean id, Bson filters){
		this.client = new MongoClient();
		this.db = client.getDatabase(dbName);
		this.c = db.getCollection(collectionName);
		if(id){
			this.projection = fields(include(projectionFields));
		} else{
			this.projection = fields(include(projectionFields), excludeId());
		}
		this.filter = filters;
		
		
	}
	
	
	public void execute() {
		List<Document> all = c.find(this.filter).projection(this.projection).into(new ArrayList<Document>());
		//print list?
		this.client.close();
	}
	
	public static class Builder {
		private String dbName;
		private String collectionName;
		private String[] fields; //projection fields
		private boolean includeID;
		private Bson filter;
		
		Builder(){
			this.dbName = "";
			this.collectionName = "";
			this.includeID = false; //should default be true or false?
			this.filter = null;
		}
		
		public MongoDBQuery build(){
			//assemble
			
			return new MongoDBQuery(this.dbName, this.collectionName, this.fields, this.includeID, this.filter); //placeholder?
		}
		public Builder setCollection(String collectionName){
			this.collectionName = collectionName;
			return this;
		}
		
		
		public Builder getFields(String ... args){
			this.fields = args;			
			return this;
		}
		
		//change name?
		public Builder getID(){
			this.includeID = true;
			return this;
		}
		
		public Builder addSearchFilter(String field, String relation, String value){
			Bson newFilter = null;
			if(relation.equals(EQUAL_TO)){
				newFilter = eq(field, value);
			} else if(relation.equals(LESS_THAN_OR_EQUAL_TO)){
				newFilter = lte(field, value);
			} else if(relation.equals(GREATER_THAN_OR_EQUAL_TO)){
				newFilter = gte(field, value);
			} else if(relation.equals(LESS_THAN)){
				newFilter = lt(field, value);
			} else if(relation.equals(GREATER_THAN)){
				newFilter = gt(field, value);
			} else if(relation.equals(NOT_EQUAL_TO)){
				newFilter = ne(field, value);
			}
			if(this.filter != null){
				this.filter = and(this.filter, newFilter);
			} else {
				this.filter = newFilter;
			}
			return this;
		}
		
		public Builder addSearchFilter(String field, String relation, float value){
			Bson newFilter = null;
			if(relation.equals(EQUAL_TO)){
				newFilter = eq(field, value);
			} else if(relation.equals(LESS_THAN_OR_EQUAL_TO)){
				newFilter = lte(field, value);
			} else if(relation.equals(GREATER_THAN_OR_EQUAL_TO)){
				newFilter = gte(field, value);
			} else if(relation.equals(LESS_THAN)){
				newFilter = lt(field, value);
			} else if(relation.equals(GREATER_THAN)){
				newFilter = gt(field, value);
			} else if(relation.equals(NOT_EQUAL_TO)){
				newFilter = ne(field, value);
			}
			if(this.filter != null){
				this.filter = and(this.filter, newFilter);
			} else {
				this.filter = newFilter;
			}
			return this;
		}
		
		public Builder addSearchFilter(String field, String relation, boolean value){
			Bson newFilter = null;
			if(relation.equals(EQUAL_TO)){
				newFilter = eq(field, value);
			} else if(relation.equals(LESS_THAN_OR_EQUAL_TO)){
				newFilter = lte(field, value);
			} else if(relation.equals(GREATER_THAN_OR_EQUAL_TO)){
				newFilter = gte(field, value);
			} else if(relation.equals(LESS_THAN)){
				newFilter = lt(field, value);
			} else if(relation.equals(GREATER_THAN)){
				newFilter = gt(field, value);
			} else if(relation.equals(NOT_EQUAL_TO)){
				newFilter = ne(field, value);
			}
			if(this.filter != null){
				this.filter = and(this.filter, newFilter);
			} else {
				this.filter = newFilter;
			}
			return this;
		}
		
		
	}
}
