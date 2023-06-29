package postgres;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.util.PGobject;


/**
 * Program inserting and querying JSON objects using PostgreSQL.
 */
public class QueryPostgres
{
	/**
	 * Connection to database
	 */
	private Connection con;
			
	/**
	 * Table name
	 */
	public static final String TABLE_NAME = "data";
	
	/**
	 * Main method
	 * 
	 * @param args
	 * 			no arguments required
	 */	
    public static void main(String [] args) throws SQLException
	{
    	QueryPostgres qpostgres = new QueryPostgres();
    	qpostgres.connect();
    	qpostgres.load();
    	System.out.println(QueryPostgres.resultSetToString(qpostgres.query(), 100));
    	qpostgres.update(3);
    	System.out.println(QueryPostgres.resultSetToString(qpostgres.query(), 100));
    	System.out.println(QueryPostgres.resultSetToString(qpostgres.query1(), 100));
    	System.out.println(QueryPostgres.resultSetToString(qpostgres.query2(), 100));
	}
        
    /**
     * Connects to Postgres database and returns connection.
     *     
     * @return
     * 		connection
     */
    public Connection connect() throws SQLException
    {    	
    	String url = "jdbc:postgresql://localhost/lab6";
		String uid = "testuser";
		String pw = "404postgrespw";
		
		System.out.println("Connecting to database.");
		con = DriverManager.getConnection(url, uid, pw);
		return con;		   
    }
    
    /**
     * Loads some sample JSON data into Postgres.
     */
    public void load() throws SQLException
    {							
		// Drop table if it exists
		Statement stmt = con.createStatement();
		stmt.executeUpdate("DROP TABLE IF EXISTS "+TABLE_NAME);
					
		// TODO: Create a table called "data"
		// Table should have field "id" as a serial primary key and field "text" as a json field		
		String createTableString = "CREATE TABLE data (id serial PRIMARY KEY, text json);";
        stmt = con.createStatement();
		stmt.executeUpdate(createTableString);

		// TODO: Add 5 objects to table of the form: key, name, num, values
		// 		- where key is an increasing integer starting at 1 (i.e. 1, 2, 3, ...)
		//		- name is "text"+key (e.g. "text1")
		//		- num is key  (e.g. 1)
		//		- values is an array of 3 objects of the form: {"val":1, "text":"text1"}, {"val":2, "text":"text2"}, {"val":3, "text":"text3"}
		//			- The example is above for key = 1, for key = 2 the values should be 2,3,4, etc.
		// Note: Use PreparedStatements!
       
        String sqlInsert = "INSERT INTO data (text) VALUES (to_json(?::json));";
		PreparedStatement preparedStatementSQLInsert = con.prepareStatement(sqlInsert);
        for (int i = 1; i < 6; i++) {
            String jasonStringArray = "{\"val\":"+i+",\"text\":\"" +("text"+i)+"\"}"; 
            String jasonString2Array = "{\"val\":"+(i+1)+",\"text\":\"" +("text"+(i+1))+ "\"}";
            String jasonString3Array = "{\"val\":"+(i+2)+",\"text\":\"" +("text"+(i+2))+ "\"}";
            String jason = "{\"key\":" + i + ",\"name\":\"" + ("text"+(i)) + "\",\"num\":" + i + ",\"values\":[" + jasonStringArray + "," + jasonString2Array + "," + jasonString3Array + "]}";
            // System.out.println("{\"key\":1,\"name\":\"text1\",\"num\":1,\"values\":[{\"val\":1,\"text\":\"text1\"},{\"val\":2,\"text\":\"text2\"},{\"val\":3,\"text\":\"text3\"}]}");
            // System.out.println(jason);
            // System.out.println(jasonStringArray);
            // System.out.println(jasonString2Array);
            // System.out.println(jasonString3Array);
            preparedStatementSQLInsert.setString(1, jason);
            preparedStatementSQLInsert.executeUpdate();
        }
        
        
        
		
	}
    
    /**
     * Updates a record with given key so that the key is 10 times bigger.  The name field should also be updated with the new key value (e.g. text10).
     */
    public void update(int key) throws SQLException
    {    	
    	// Two choices: 
		// 1) Retrieve JSON objectd given database key. Update JSON document in Java code. Then write back.
		// 2) Postgres v14 supports UPDATE directly on Java document.
        int newValue = key * 10;
        int i = key;
        String updateQuery = "UPDATE " + TABLE_NAME + " SET text = to_json(?::json) WHERE id = ?";
        PreparedStatement preparedStatementSQLInsert = con.prepareStatement(updateQuery);
        String jasonStringArray = "{\"val\":"+i+",\"text\":\"" +("text"+i)+"\"}"; 
        String jasonString2Array = "{\"val\":"+(i+1)+",\"text\":\"" +("text"+(i+1))+ "\"}";
        String jasonString3Array = "{\"val\":"+(i+2)+",\"text\":\"" +("text"+(i+2))+ "\"}";
        String jason = "{\"key\":" + newValue + ",\"name\":\"" + ("text"+(newValue)) + "\",\"num\":" + i + ",\"values\":[" + jasonStringArray + "," + jasonString2Array + "," + jasonString3Array + "]}";
        preparedStatementSQLInsert.setString(1, jason);
        preparedStatementSQLInsert.setInt(2, key);
        preparedStatementSQLInsert.executeUpdate();
    }
    
    /**
     * Performs a query that prints out all data.
     */
	public ResultSet query() throws SQLException
	{		
		Statement stmt = con.createStatement();
		return stmt.executeQuery("SELECT * FROM "+TABLE_NAME);
	}
    
    /**
     * Performs a query that returns all documents with key < 4.  Only show the key, name, and num fields.  Name the output field as "output".
     */
    public ResultSet query1() throws SQLException
    {
    	// TODO: Write a query that returns all documents with key < 4.  Only show the key, name, and num fields.
    	// See: https://www.postgresql.org/docs/14/functions-json.html
    	// See: https://www.postgresql.org/docs/14/datatype-json.html
    	// Note: You will need to use cast() to convert 'num' field to an int to do the comparison.
    	// Note: You may need to use json_build_object().
        String querySQL = "SELECT JSON_BUILD_OBJECT('key', (text -> 'key'), 'name', (text -> 'name'), 'num', (text -> 'num')) AS Output FROM data WHERE (data.text ->> 'key')::int < 4;";
        try {
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(querySQL);
            return resultSet;
        } catch (SQLException exception) {
            System.out.println(exception);
        }
    	return null;
    }
    
    /**
     * Performs a query that returns all documents with key > 2 OR contains an element in the array with val = 4. 
     */
    public ResultSet query2() throws SQLException
    {    	
    	// TODO: Write a query that returns all documents with key > 2 OR contains an element in the array with val = 4. 
    	// See: https://www.postgresql.org/docs/14/functions-json.html
    	// See: https://www.postgresql.org/docs/14/datatype-json.html
    	// Note: You will need to use cast() to convert 'key' field to an int to do the comparison.
    	// Note: Getting the values out of the JSON array values may require an SQL subquery and the method json_array_elements().
        // (data.text ->> 'key')::int > 2 OR 
        String querySQL1 = "SELECT (((text ->>'values')::json -> 0) -> 'val'), (((text ->>'values')::json -> 1) -> 'val'), (((text ->>'values')::json -> 2) -> 'val') FROM data";
        String querySQL  = "SELECT text As output FROM data WHERE (data.text ->> 'key')::int > 2 OR (((text ->>'values')::JSONB -> 0) -> 'val')::int = 4 OR (((text ->>'values')::JSONB -> 1) -> 'val')::int = 4 OR (((text ->>'values')::JSONB -> 2) -> 'val')::int = 4";
        
        try {
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(querySQL);
            return resultSet;
        } catch (SQLException exception) {
            System.out.println(exception);
        }
    	return null;  	
    }          
    
        
    /*
	 * Do not change anything below here.
	 */
	/**
     * Converts a ResultSet to a string with a given number of rows displayed.
     * Total rows are determined but only the first few are put into a string.
     * 
     * @param rst
     * 		ResultSet
     * @param maxrows
     * 		maximum number of rows to display
     * @return
     * 		String form of results
     * @throws SQLException
     * 		if a database error occurs
     */    
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException
    {                       
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
                
        while (rst.next()) 
        {
            if (rowCount < maxrows)
            {
                for (int j = 0; j < meta.getColumnCount(); j++) 
                { 
                	Object obj = rst.getObject(j + 1);                	 	                       	                                	
                	buf.append(obj);                    
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");                    
                }
                buf.append('\n');
            }
            rowCount++;
        }            
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }
} 
