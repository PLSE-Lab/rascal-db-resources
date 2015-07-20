package databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

public class MongoDBQuery {
	
	//add static String constants to define the relational operators
//	public static final String EQUAL_TO = "=";
//	public static final String NOT_EQUAL_TO = "!=";
//	public static final String LESS_THAN = "<";
//	public static final String LESS_THAN_OR_EQUAL_TO = "<=";
//	public static final String GREATER_THAN = ">";
//	public static final String GREATER_THAN_OR_EQUAL_TO = ">=";
	
	private MongoDatabase db;
	private MongoCollection<Document> c;
	private Bson filter;
	private Bson projection;
	private MongoClient client;
	
	
	
	MongoDBQuery(String dbName, String collectionName, Set<String> projectionFields, boolean id, Bson filters){
		this.client = new MongoClient();
		this.db = client.getDatabase(dbName);
		this.c = db.getCollection(collectionName);
		String[] temp = new String[projectionFields.size()]; 
		temp = projectionFields.toArray(temp);
		if(id){
			this.projection = fields(include(temp));
		} else{
			this.projection = fields(include(temp), excludeId());
		}
		this.filter = filters;
		
		
	}
	
	
	public List<Document> execute() {
		List<Document> all = null;
		if(this.filter == null && this.projection == null){
			all = c.find().into(new ArrayList<Document>());
		} else if(this.filter == null && this.projection != null){
			all = c.find().projection(this.projection).into(new ArrayList<Document>());
		} else if(this.filter != null && this.projection == null){
			all = c.find(this.filter).into(new ArrayList<Document>());
		} else {
			all = c.find(this.filter).projection(this.projection).into(new ArrayList<Document>());
		}
		//print list?
		this.client.close();
		return all;
	}
	
	public static class Builder {
		private String dbName;
		private String collectionName;
		private Set<String> fields = new HashSet<String>(); //projection fields
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
		public Builder setDatabase(String databaseName){
			this.dbName = databaseName;
			return this;
		}
		public Builder setCollection(String collectionName){
			this.collectionName = collectionName;
			return this;
		}
		
		
		public Builder getFields(String ... args){
			for(int i = 0; i < args.length; i++){
				this.fields.add(args[i]);
			}
			return this;
		}
		
		//change name?
		public Builder getID(){
			this.includeID = true;
			return this;
		}
		
		public Builder addSearchFilter(String field, Operator relation, String value){
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
			
			if(this.filter != null){
				this.filter = and(this.filter, newFilter);
			} else {
				this.filter = newFilter;
			}
			
			return this;
		}
		
		public Builder addSearchFilter(String field, Operator relation, float value){
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
			
			if(this.filter != null){
				this.filter = and(this.filter, newFilter);
			} else {
				this.filter = newFilter;
			}
			
			return this;
		}
		
		public Builder addSearchFilter(String field, Operator relation, boolean value){
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
			
			if(this.filter != null){
				this.filter = and(this.filter, newFilter);
			} else {
				this.filter = newFilter;
			}
			
			return this;
		}
		
		
	}
}
