package junit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.Test;

import postgres.QueryPostgres;


/**
 * Tests Postgres queries and methods.
 */
public class TestPostgres
{
	/**
	 * Class being tested
	 */
	private static QueryPostgres q;
			
	
	/**
	 * Requests a connection to the Postgres server.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@BeforeClass
	public static void init() throws Exception 
	{		
		q = new QueryPostgres();		
		q.connect();
	}		
		    
    /**
     * Tests load.
     */
    @Test
    public void testLoad() throws SQLException
    {   
    	// Load data
    	q.load();
    	
    	// Perform query to get data
    	String result = QueryPostgres.resultSetToString(q.query(), 100);
    	
    	String answer = "Total columns: 2"
    			+"\nid, text"
    			+"\n1, {\"key\":1,\"name\":\"text1\",\"num\":1,\"values\":[{\"val\":1,\"text\":\"text1\"},{\"val\":2,\"text\":\"text2\"},{\"val\":3,\"text\":\"text3\"}]}"
    			+"\n2, {\"key\":2,\"name\":\"text2\",\"num\":2,\"values\":[{\"val\":2,\"text\":\"text2\"},{\"val\":3,\"text\":\"text3\"},{\"val\":4,\"text\":\"text4\"}]}"
    			+"\n3, {\"key\":3,\"name\":\"text3\",\"num\":3,\"values\":[{\"val\":3,\"text\":\"text3\"},{\"val\":4,\"text\":\"text4\"},{\"val\":5,\"text\":\"text5\"}]}"
    			+"\n4, {\"key\":4,\"name\":\"text4\",\"num\":4,\"values\":[{\"val\":4,\"text\":\"text4\"},{\"val\":5,\"text\":\"text5\"},{\"val\":6,\"text\":\"text6\"}]}"
    			+"\n5, {\"key\":5,\"name\":\"text5\",\"num\":5,\"values\":[{\"val\":5,\"text\":\"text5\"},{\"val\":6,\"text\":\"text6\"},{\"val\":7,\"text\":\"text7\"}]}"
    			+"\nTotal results: 5";
    	
    	System.out.println("Result after load:\n"+result);
    	assertEquals(answer, result);    	
    }
    
    /**
     * Tests update.
     */
    @Test
    public void testUpdate() throws SQLException
    {   
    	// Perform update
    	q.update(3);
    	
    	// Perform query to get data
    	String result = QueryPostgres.resultSetToString(q.query(), 100);
    	
    	String answer = "Total columns: 2"
    					+"\nid, text"
    					+"\n1, {\"key\":1,\"name\":\"text1\",\"num\":1,\"values\":[{\"val\":1,\"text\":\"text1\"},{\"val\":2,\"text\":\"text2\"},{\"val\":3,\"text\":\"text3\"}]}"
    					+"\n2, {\"key\":2,\"name\":\"text2\",\"num\":2,\"values\":[{\"val\":2,\"text\":\"text2\"},{\"val\":3,\"text\":\"text3\"},{\"val\":4,\"text\":\"text4\"}]}"
    					+"\n4, {\"key\":4,\"name\":\"text4\",\"num\":4,\"values\":[{\"val\":4,\"text\":\"text4\"},{\"val\":5,\"text\":\"text5\"},{\"val\":6,\"text\":\"text6\"}]}"
    					+"\n5, {\"key\":5,\"name\":\"text5\",\"num\":5,\"values\":[{\"val\":5,\"text\":\"text5\"},{\"val\":6,\"text\":\"text6\"},{\"val\":7,\"text\":\"text7\"}]}"
    					+"\n3, {\"key\":30,\"name\":\"text30\",\"num\":3,\"values\":[{\"val\":3,\"text\":\"text3\"},{\"val\":4,\"text\":\"text4\"},{\"val\":5,\"text\":\"text5\"}]}"
    					+"\nTotal results: 5";
    	
    	System.out.println("Result after update:\n"+result);
    	assertEquals(answer, result);    	    	  
    }
    
    /**
     * Tests query #1. 
     */
    @Test
    public void testQuery1() throws SQLException
    {       
    	// Load data (to ensure have no data changed)
    	q.load();
    	
    	// Perform query to get data
    	String result = QueryPostgres.resultSetToString(q.query1(), 100);
    	
    	String answer = "Total columns: 1"
    			+"\noutput"
				+"\n{\"key\" : 1, \"name\" : \"text1\", \"num\" : 1}"
				+"\n{\"key\" : 2, \"name\" : \"text2\", \"num\" : 2}"
				+"\n{\"key\" : 3, \"name\" : \"text3\", \"num\" : 3}"
				+"\nTotal results: 3";    					
				
    	System.out.println("Result for query 1:\n"+result);
    	assertEquals(answer, result);    	    	  
    }
    
    /**
     * Tests query #2.  
     */
    @Test
    public void testQuery2() throws SQLException
    {       	    	
    	// Load data (to ensure have no data changed)
    	q.load();
    	
    	// Perform query to get data
    	String result = QueryPostgres.resultSetToString(q.query2(), 100);
    	
    	String answer = "Total columns: 1"+"\noutput"    					
    					+"\n\\{\"key\":2,\"name\":\"text2\",\"num\":2,\"values\":\\[\\{\"val\":2,\"text\":\"text2\"\\},\\{\"val\":3,\"text\":\"text3\"\\},\\{\"val\":4,\"text\":\"text4\"\\}\\]\\}"
    					+"\n\\{\"key\":3,\"name\":\"text3\",\"num\":3,\"values\":\\[\\{\"val\":3,\"text\":\"text3\"\\},\\{\"val\":4,\"text\":\"text4\"\\},\\{\"val\":5,\"text\":\"text5\"\\}\\]\\}"
    					+"\n\\{\"key\":4,\"name\":\"text4\",\"num\":4,\"values\":\\[\\{\"val\":4,\"text\":\"text4\"\\},\\{\"val\":5,\"text\":\"text5\"\\},\\{\"val\":6,\"text\":\"text6\"\\}\\]\\}"
    					+"\n\\{\"key\":5,\"name\":\"text5\",\"num\":5,\"values\":\\[\\{\"val\":5,\"text\":\"text5\"\\},\\{\"val\":6,\"text\":\"text6\"\\},\\{\"val\":7,\"text\":\"text7\"\\}\\]\\}"
    					+"\nTotal results: 4";    
    	
    	System.out.println("Result for query 2:\n"+result);
    	
    	// Verify result
    	Pattern pattern = Pattern.compile(answer, Pattern.DOTALL);
		Matcher matcher = pattern.matcher(result);
		boolean b = matcher.matches();				
    	if (!b)
    		fail("Output does not match.");
    }
}
