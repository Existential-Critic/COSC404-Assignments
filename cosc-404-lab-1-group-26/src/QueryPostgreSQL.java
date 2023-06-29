import java.math.BigDecimal;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Performs CREATE, INSERT, DELETE, and SELECT on a PostgreSQL database.
 */
public class QueryPostgreSQL 
{
	/**
	 * Connection to database
	 */
	private Connection con;
	
	
	/**
	 * Main method is only used for convenience.  Use JUnit test file to verify your answer.
	 * 
	 * @param args
	 * 		none expected
	 * @throws SQLException
	 * 		if a database error occurs
	 */
	public static void main(String[] args) throws SQLException
	{
		QueryPostgreSQL q = new QueryPostgreSQL();
		q.connect();	
		q.drop();
		q.create();
		q.insert();	
		q.delete();
		System.out.println(QueryPostgreSQL.resultSetToString(q.query1(), 1000));
		System.out.println(QueryPostgreSQL.resultSetToString(q.query2(), 1000));
		System.out.println(QueryPostgreSQL.resultSetToString(q.query3(), 1000));
		q.close();
	}

	/**
	 * Makes a connection to the database and returns connection to caller.
	 * 
	 * @return
	 * 		connection
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public Connection connect() throws SQLException
	{
		String url = "jdbc:postgresql://localhost/lab1";
		String uid = "testuser";
		String pw = "404postgrespw";
		
		System.out.println("Connecting to database.");
		// Note: Must assign connection to instance variable as well as returning it back to the caller
		// TODO: Make a connection to the database and store connection in con variable before returning it.
		try 
		{
			con = DriverManager.getConnection(url, uid, pw);
		} catch (SQLException ex) 
		{
			System.out.println(ex);
		}
		return con;	                       
	}
	
	/**
	 * Closes connection to database.
	 */
	public void close()
	{
		System.out.println("Closing database connection.");
		// TODO: Close the database connection.  Catch any exception and print out if it occurs.	
		try 
		{
			con.close();
		} catch (SQLException ex)
		{
			System.out.println(ex);
		} 
	}
	
	/**
	 * Drops the table from the database.  If table does not exist, error is ignored.
	 */
	public void drop()
	{
		System.out.println("Dropping table person.");
		// TODO: Drop the table person.  Catch any exception and print out if it occurs.
		try 
		{
			String sqlStatment = "DROP TABLE person";
			Statement stmt = con.createStatement();
			stmt.execute(sqlStatment);
		} catch (SQLException ex) 
		{
			// TODO: handle exception
			System.out.println(ex);
		}	
	}
	
	/**
	 * Creates the table in the database.  Table name: person
	 * Fields:
	 *  - id - integer, must auto-increment
	 *  - name - variable character field up to size 40
	 *  - salary - must hold up to 99,999,999.99 exactly
	 *  - birthdate - date
	 *  - last_update - datetime
	 */
	public void create() throws SQLException
	{
		System.out.println("Creating table person.");
		// TODO: Create the table person.	
		String createTableString = "CREATE TABLE person (id serial PRIMARY KEY, name VARCHAR (40), salary numeric(10, 2), birthdate DATE, last_update timestamp);";
		Statement stmt = con.createStatement();
		stmt.executeUpdate(createTableString);
	}
	
	/**
	 * Inserts the test records in the database.  Must used a PreparedStatement.  
	 * 
	 * Data:
	 * Names = "Ann Alden", "Bob Baron", "Chloe Cat", "Don Denton", "Eddy Edwards"
	 * Salaries = "123000", "225423", "99999999.99", "91234.24", "55125125.25"
	 * Birthdates = "1986-03-04", "1993-12-02", "1999-01-15", "2004-08-03", "2003-05-17"
	 * Last_updates = "2022-01-04 11:30:30", "2022-01-04 12:30:25", "2022-01-04 12:25:45", "2022-01-04 12:45:00", "2022-01-05 23:00:00"
	 */
	public void insert() throws SQLException
	{
		System.out.println("Inserting records.");
		// TODO: Insert records using a PreparedStatement.
		String sqlInsert = "INSERT INTO person (name, salary, birthdate, last_update) VALUES (?, ?, ?, ?);";
		PreparedStatement preparedStatementSQLInsert = con.prepareStatement(sqlInsert);
		//	"Ann Alden"
		preparedStatementSQLInsert.setString(1, "Ann Alden");
		preparedStatementSQLInsert.setBigDecimal(2, BigDecimal.valueOf(123000));
		preparedStatementSQLInsert.setDate(3, java.sql.Date.valueOf("1986-03-04"));
		preparedStatementSQLInsert.setTimestamp(4, java.sql.Timestamp.valueOf("2022-01-04 11:30:30"));
		preparedStatementSQLInsert.executeUpdate();

		// "Bob Baron"
		preparedStatementSQLInsert.setString(1, "Bob Baron");
		preparedStatementSQLInsert.setBigDecimal(2, BigDecimal.valueOf(225423.00));
		preparedStatementSQLInsert.setDate(3, java.sql.Date.valueOf("1993-12-02"));
		preparedStatementSQLInsert.setTimestamp(4, java.sql.Timestamp.valueOf("2022-01-04 12:30:25"));
		preparedStatementSQLInsert.executeUpdate();

		// "Chloe Cat"
		preparedStatementSQLInsert.setString(1, "Chloe Cat");
		preparedStatementSQLInsert.setBigDecimal(2, BigDecimal.valueOf(99999999.99));
		preparedStatementSQLInsert.setDate(3, java.sql.Date.valueOf("1999-01-15"));
		preparedStatementSQLInsert.setTimestamp(4, java.sql.Timestamp.valueOf("2022-01-04 12:25:45"));
		preparedStatementSQLInsert.executeUpdate();

		// "Don Denton"
		preparedStatementSQLInsert.setString(1, "Don Denton");
		preparedStatementSQLInsert.setBigDecimal(2, BigDecimal.valueOf(91234.24));
		preparedStatementSQLInsert.setDate(3, java.sql.Date.valueOf("2004-08-03"));
		preparedStatementSQLInsert.setTimestamp(4, java.sql.Timestamp.valueOf("2022-01-04 12:45:00"));
		preparedStatementSQLInsert.executeUpdate();

		// "Eddy Edwards"
		preparedStatementSQLInsert.setString(1, "Eddy Edwards");
		preparedStatementSQLInsert.setBigDecimal(2, BigDecimal.valueOf(55125125.25));
		preparedStatementSQLInsert.setDate(3, java.sql.Date.valueOf("2003-05-17"));
		preparedStatementSQLInsert.setTimestamp(4, java.sql.Timestamp.valueOf("2022-01-05 23:00:00"));
		preparedStatementSQLInsert.executeUpdate();
	}
	
	/**
	 * Delete the row where person name is 'Bob Baron'.
	 * 
	 * @return
	 * 		number of rows deleted
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public int delete() throws SQLException
	{
		System.out.println("Deleting a record.");
		// TODO: Delete record where name is 'Bob Baron'
		try 
		{
			String sqlDelete = "DELETE FROM person WHERE name = ?";
			PreparedStatement preparedStatementSQLDelete = con.prepareStatement(sqlDelete);
			preparedStatementSQLDelete.setString(1, "Bob Baron");
			int numberOfRowsDeleted = preparedStatementSQLDelete.executeUpdate();
			System.out.println(numberOfRowsDeleted);
			return numberOfRowsDeleted;
		} catch (SQLException ex) 
		{
			// TODO: handle exception
			System.out.println(ex);
		}
		return 0;		
	}
	
	/**
	 * Query returns the person name and salary where rows are sorted by salary descending.
	 * 
	 * @return
	 * 		ResultSet
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public ResultSet query1() throws SQLException
	{
		System.out.println("Executing query #1.");
		// TODO: Write SQL query
		try 
		{
			String sqlQuery1 = "SELECT name, salary FROM person ORDER BY salary DESC";
			PreparedStatement preparedStatementSQLQuery1 = con.prepareStatement(sqlQuery1);
			ResultSet resultSet = preparedStatementSQLQuery1.executeQuery();
			return resultSet;
		} catch (SQLException ex) 
		{
			// TODO: handle exception
			System.out.println(ex);
		}
		return null;				
	}
	
	/**
	 * Query returns the person last name and salary if the person's salary is greater than the average salary of all persons.
	 * 
	 * @return
	 * 		ResultSet
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public ResultSet query2() throws SQLException
	{
		System.out.println("Executing query #2.");
		// TODO: Write SQL query
		try 
		{		
			String sqlQuery2AvgSalary = "SELECT lastname, salary FROM (SELECT split_part(name, ' ', 1) AS firstname, split_part(name, ' ', 2) AS lastname, salary FROM person WHERE salary > (SELECT AVG(salary) FROM person)) AS firstTable";
			Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet resultSet = stmt.executeQuery(sqlQuery2AvgSalary);
			return resultSet;	
		} catch (SQLException ex) 
		{
			// TODO: handle exception
			System.out.println(ex);
		}
		return null;		
	}

	/**
 	 * Query returns all fields of a pair of people where a pair of people is returned if the last_update field of their records have been updated less than an hour apart.
	 * Do not duplicate pairs.  Example: Only show (Ann, Bob) and not also (Bob, Ann).
	 * 
	 * @return
	 * 		ResultSet
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public ResultSet query3() throws SQLException
	{
		System.out.println("Executing query #3.");
		// TODO: Write SQL query
		try 
		{
			String sqlQuery3 = "SELECT * FROM person AS person1 JOIN person AS person2 ON person1.id != person2.id WHERE person1.name < person2.name AND EXTRACT(EPOCH FROM (person2.last_update - person1.last_update)) < 3600";
			//String sqlQuery35 = "SELECT person1.name, person1.salary, person1.birthdate, person1.last_update, person2.name, person2.salary, person2.birthdate, person2.last_update FROM (" + sqlQuery3 + ") foo";
			PreparedStatement preparedStatementSQLQuery3 = con.prepareStatement(sqlQuery3, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			ResultSet resultSet = preparedStatementSQLQuery3.executeQuery();
			/*while(resultSet.next())
			{
				long differenceBetweenOneTwo = resultSet.getTimestamp(5).getTime() - resultSet.getTimestamp(10).getTime();
				long absoultDifferenceBetweenOneTwo = Math.abs(differenceBetweenOneTwo);
				if (absoultDifferenceBetweenOneTwo/3600000 >= 1) {
					resultSet.deleteRow();
				}
			}*/
			return resultSet;
		} catch (SQLException ex) 
		{
			// TODO: handle exception
			System.out.println(ex);
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
	if (rst == null)
		return "No resultset";
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
    
    /**
     * Converts ResultSetMetaData into a string.
     * 
     * @param meta
     * 		 ResultSetMetaData
     * @return
     * 		string form of metadata
     * @throws SQLException
     * 		if a database error occurs
     */
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException
    {
	    StringBuffer buf = new StringBuffer(5000);                                   
	    buf.append(meta.getColumnName(1)+" ("+meta.getColumnLabel(1)+", "+meta.getColumnType(1)+"-"+meta.getColumnTypeName(1)+", "+meta.getColumnDisplaySize(1)+", "+meta.getPrecision(1)+", "+meta.getScale(1)+")");
	    for (int j = 2; j <= meta.getColumnCount(); j++)
	        buf.append(", "+meta.getColumnName(j)+" ("+meta.getColumnLabel(j)+", "+meta.getColumnType(j)+"-"+meta.getColumnTypeName(j)+", "+meta.getColumnDisplaySize(j)+", "+meta.getPrecision(j)+", "+meta.getScale(j)+")");
	    return buf.toString();
    }
}
