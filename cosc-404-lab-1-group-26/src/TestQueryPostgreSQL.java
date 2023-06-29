import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;


public class TestQueryPostgreSQL 
{
	/**
	 * Class being tested
	 */
	private static QueryPostgreSQL q;
	
	/**
	 * Connection to the database
	 */
	private static Connection con;
	
	/**
	 * Requests a connection to the database.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@BeforeAll
	public static void init() throws Exception 
	{		
		q = new QueryPostgreSQL();
		con = q.connect();					
	}
	
	/**
	 * Closes connection.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@AfterAll
	public static void end() throws Exception 
	{
		q.close();
        
	}
	
	/**
     * Tests drop command.
     */
    @Test
    public void testDrop() 
    {    
    	q.drop();
    	
    	// See if table exists
    	try
    	{
	    	Statement stmt = con.createStatement();
	    	stmt.executeQuery("SELECT * FROM person");
	    	fail("Table person exists and should be dropped!");
    	}
    	catch (SQLException e)
    	{
    		System.out.println(e);
    	}
    }
    
    /**
     * Tests create.
     */
    @Test
    public void testCreate() throws SQLException
    {   
    	q.drop();
    	q.create();
    	
    	// See if table exists
    	try
    	{
	    	Statement stmt = con.createStatement();
	    	ResultSet rst = stmt.executeQuery("SELECT * FROM person");	    	
	    	
	    	// Verify its metadata
	    	ResultSetMetaData rsmd = rst.getMetaData();
	    	String st = QueryPostgreSQL.resultSetMetaDataToString(rsmd);
	    	System.out.println(st);	    			
	    	assertEquals("id (id, 4-serial, 11, 10, 0), name (name, 12-varchar, 40, 40, 0), salary (salary, 2-numeric, 12, 10, 2), birthdate (birthdate, 91-date, 13, 13, 0), last_update (last_update, 93-timestamp, 29, 29, 6)", st);	    	
    	}
    	catch (SQLException e)
    	{
    		System.out.println(e);
    		fail("Table person does not exist!");
    	}
    }
    
    /**
     * Tests insert.
     */
    @Test
    public void testInsert() throws SQLException
    {    
    	q.drop();
    	q.create();
    	q.insert();
    	
    	// Verify data was inserted properly
    	String answer = "Total columns: 5"
				+"\nid, name, salary, birthdate, last_update"
				+"\n1, Ann Alden, 123000.00, 1986-03-04, 2022-01-04 11:30:30.0"
				+"\n2, Bob Baron, 225423.00, 1993-12-02, 2022-01-04 12:30:25.0"
				+"\n3, Chloe Cat, 99999999.99, 1999-01-15, 2022-01-04 12:25:45.0"
				+"\n4, Don Denton, 91234.24, 2004-08-03, 2022-01-04 12:45:00.0"
				+"\n5, Eddy Edwards, 55125125.25, 2003-05-17, 2022-01-05 23:00:00.0"
				+"\nTotal results: 5";	
    	runSQLQuery("SELECT * FROM person", answer);    	
    }
    
    /**
     * Tests delete.  
     */
    @Test
    public void testDelete() throws SQLException
    {    
    	q.drop();
    	q.create();
    	q.insert();
    	q.delete();
    	
    	// Verify data was deleted properly
    	String answer = "Total columns: 5"
				+"\nid, name, salary, birthdate, last_update"
				+"\n1, Ann Alden, 123000.00, 1986-03-04, 2022-01-04 11:30:30.0"				
				+"\n3, Chloe Cat, 99999999.99, 1999-01-15, 2022-01-04 12:25:45.0"
				+"\n4, Don Denton, 91234.24, 2004-08-03, 2022-01-04 12:45:00.0"
				+"\n5, Eddy Edwards, 55125125.25, 2003-05-17, 2022-01-05 23:00:00.0"
				+"\nTotal results: 4";	
    	runSQLQuery("SELECT * FROM person", answer);    	
    }
    
    /**
     * Tests first query.
     */
    @Test
    public void testQuery1() throws SQLException
    {    
    	q.drop();
    	q.create();
    	q.insert();
    	q.delete();
    	
    	ResultSet rst = q.query1();
    	
    	// Verify result
    	String answer = "Total columns: 2"
				+"\nname, salary"
				+"\nChloe Cat, 99999999.99"
				+"\nEddy Edwards, 55125125.25"			
				+"\nAnn Alden, 123000.00"
				+"\nDon Denton, 91234.24"
				+"\nTotal results: 4";
    	String queryResult = QueryPostgreSQL.resultSetToString(rst, 100);
    	System.out.println(queryResult);
    	assertEquals(answer, queryResult);    	    
    }
    
    /**
     * Tests second query.
     */
    @Test
    public void testQuery2() throws SQLException
    {    
    	q.drop();
    	q.create();
    	q.insert();
    	q.delete();
    	
    	ResultSet rst = q.query2();
    	
    	// Verify result
    	String answer = "Total columns: 2"
				+"\nlastname, salary"
				+"\nCat, 99999999.99"
				+"\nEdwards, 55125125.25"				
				+"\nTotal results: 2";
    	String queryResult = QueryPostgreSQL.resultSetToString(rst, 100);
    	System.out.println(queryResult);
    	assertEquals(answer, queryResult);   
    }
    
    /**
     * Tests third query.
     */
    @Test
    public void testQuery3() throws SQLException
    {    
    	q.drop();
    	q.create();
    	q.insert();
    	q.delete();
    	
    	ResultSet rst = q.query3();
    	
    	// Verify result
    	String answer = "Total columns: 10"
				+"\nid, name, salary, birthdate, last_update, id, name, salary, birthdate, last_update"
				+"\n1, Ann Alden, 123000.00, 1986-03-04, 2022-01-04 11:30:30.0, 3, Chloe Cat, 99999999.99, 1999-01-15, 2022-01-04 12:25:45.0"
				+"\n3, Chloe Cat, 99999999.99, 1999-01-15, 2022-01-04 12:25:45.0, 4, Don Denton, 91234.24, 2004-08-03, 2022-01-04 12:45:00.0"
				+"\nTotal results: 2";
    	String queryResult = QueryPostgreSQL.resultSetToString(rst, 100);
    	System.out.println(queryResult);
    	assertEquals(answer, queryResult);   
    }    
    
    /**
     * Runs an SQL query and compares answer to expected answer.  
     * 
     * @param sql
     * 		SQL query
     * @param answer
     * 		expected answer          
     */
    public static void runSQLQuery(String sql, String answer)
    {    	 
         try
         {
        	Statement stmt = con.createStatement();
 	    	ResultSet rst = stmt.executeQuery(sql);	    	
 	    	
 	    	String st = QueryPostgreSQL.resultSetToString(rst, 1000);
 	    	System.out.println(st);	    			
 	    		
 	    	assertEquals(answer, st);	           	             
            
 	    	stmt.close();
         }            
         catch (SQLException e)
         {	
        	 System.out.println(e);
        	 fail("Incorrect exception: "+e);
         }              
    }
}
