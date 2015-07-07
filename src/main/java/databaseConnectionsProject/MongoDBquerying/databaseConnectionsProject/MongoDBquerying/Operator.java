package databaseConnectionsProject.MongoDBquerying.databaseConnectionsProject.MongoDBquerying;

public enum Operator {
	EQUAL_TO ("="),
	NOT_EQUAL_TO ("!="),
	LESS_THAN ("<"),
	LESS_THAN_OR_EQUAL_TO ("<="),
	GREATER_THAN (">"),
	GREATER_THAN_OR_EQUAL_TO (">=");
	//not sure if  I actually need these strings, leaving just in case
	private String expression; 
	Operator(String express){
		this.expression = express;
	}
	public String toString(){
		return this.expression;
	}
}
