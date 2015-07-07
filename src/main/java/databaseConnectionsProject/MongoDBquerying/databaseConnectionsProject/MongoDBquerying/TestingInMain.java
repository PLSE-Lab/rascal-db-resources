package databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying;

import java.sql.SQLException;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class TestingInMain {
public static void main(String[] args){
		
        testingMySQL();
        testingMongo();
        
	}
	
	public static void testingMySQL(){
		String type = MySQLQuery.Builder.SELECT_SPECIFIC;
		String host = "localhost";
		String dbName = "feedback";
		String username = "sqluser";
		String password = "sqluserpw";
		MySQLQuery.Builder builder = new MySQLQuery.Builder(type, host, dbName, username, password); 
		builder.getColumn("summary");
		builder.setTable("comments");
		builder.getColumn("email");
		builder.where("id=1");
		builder.getColumn("comments");
		//builder.getColumn("one", "two", "three", "four", "five");
		MySQLQuery query = builder.build();
		System.out.println(query.toString());
		try {
			query.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void testingMongo(){
		MongoDBQuery.Builder builder = new MongoDBQuery.Builder();
        builder.setDatabase("test");
        builder.setCollection("insertTest");
        builder.getFields("i");
        builder.addSearchFilter("x", Operator.EQUAL_TO, 0);
        MongoDBQuery query = builder.build();
        List<Document> results = query.execute();
        for(Document current: results){
        	System.out.println(current);
        }
	}
}
