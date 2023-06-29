import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


// Performs CREATE, INSERT, DELETE, and SELECT on a MySQL database.
public class QueryMySQL {
	// Connection to database
	private Connection con;
	
	/**
	 * Main method is only used for convenience.  Use JUnit test file to verify your answer.
	 * @param args
	 * 		none expected
	 * @throws SQLException
	 * 		if a database error occurs
	 */
	public static void main(String[] args) throws SQLException {
		QueryMySQL q = new QueryMySQL();
		q.connect();	
		q.drop();
		q.create();
		q.insert();	
		q.delete();		
		System.out.println(QueryMySQL.resultSetToString(q.query1(), 1000));
		System.out.println(QueryMySQL.resultSetToString(q.query2(), 1000));
		System.out.println(QueryMySQL.resultSetToString(q.query3(), 1000));
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
	public Connection connect() throws SQLException {
		String url = "jdbc:mysql://localhost/lab1";
		String uid = "testuser";
		String pw = "404testpw";
		
		System.out.println("Connecting to database.");		
		// Note: Must assign connection to instance variable as well as returning it back to the caller
		con = DriverManager.getConnection(url, uid, pw);
		// Print a confirmation messages that the connection was established.
		System.out.println("Connection to database established.");
		return con;	                       
	}
	
	// Closes connection to database.
	public void close() {
		System.out.println("Closing database connection.");
		try {
			// Close the connection.
			con.close();
			// Print a confirmation message that connection was closed.
			System.out.println("Connection to database closed.");
		}
		catch (SQLException e) {
			// Print an error message if the connection failed to close.
			System.out.println("Error, connection to database not closed.");
		}
	}
	
	// Drops the table from the database.  If table does not exist, error is ignored.
	public void drop() {
		System.out.println("Dropping table person.");
		try {
			// Create an empty statement.
			Statement stmt = con.createStatement();
			// Execute the DROP TABLE command on table person
			stmt.execute("DROP TABLE person");
			// Print a confirmation message that table person was dropped.
			System.out.println("Table person dropped.");
		}
		catch (SQLException e) {
			// Print an error message if table person failed to drop.
			System.out.println("Error, table person not dropped.");
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
	public void create() throws SQLException {
		System.out.println("Creating table person.");
		try {
			// Create an empty statement.
			Statement stmt = con.createStatement();
			// Create the string containing the SQL command
			String createSQL = """
				CREATE TABLE person 
				(id INTEGER NOT NULL AUTO_INCREMENT, 
				name VARCHAR(40), 
				salary DECIMAL(10,2), 
				birthdate DATE, 
				last_update TIMESTAMP, 
				PRIMARY KEY(id));
				""";
			// Execute the SQL command in the statement.
			stmt.executeUpdate(createSQL);
			// Print a confirmation message that table person was created.
			System.out.println("Table person created in database.");
		}
		catch (SQLException e) {
			// Print an error message if table person failed to be created.
			System.out.println("Error, table person not created.");
		}
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
	public void insert() throws SQLException {
		System.out.println("Inserting records.");
		try {
			// Create arrays holding the pre-existing values
			String[] names = {"Ann Alden","Bob Baron","Chloe Cat","Don Denton","Eddy Edwards"};
			Double[] salaries = {123000.00,225423.00,99999999.99,91234.24,55125125.25};
			java.sql.Date[] birthdates = {java.sql.Date.valueOf("1986-03-04"),java.sql.Date.valueOf("1993-12-02"),java.sql.Date.valueOf("1999-01-15"),java.sql.Date.valueOf("2004-08-03"),java.sql.Date.valueOf("2003-05-17")};
			java.sql.Timestamp[] lastUpdates = {java.sql.Timestamp.valueOf("2022-01-04 11:30:30.0"),java.sql.Timestamp.valueOf("2022-01-04 12:30:25.0"),java.sql.Timestamp.valueOf("2022-01-04 12:25:45.0"),java.sql.Timestamp.valueOf("2022-01-04 12:45:00.0"),java.sql.Timestamp.valueOf("2022-01-05 23:00:00.0")};
			// Create the prepared statement with the command to insert five records
			PreparedStatement pstmt = con.prepareStatement("""
				INSERT INTO person (name,salary,birthdate,last_update)
				VALUES
				(?,?,?,?),
				(?,?,?,?),
				(?,?,?,?),
				(?,?,?,?),
				(?,?,?,?)
				""");
			// For loop to set all values in pstmt
			for(int i = 0;i < 5;i++) {
				pstmt.setString((1+(i*4)),names[i]);
				pstmt.setDouble((2+(i*4)),salaries[i]);
				pstmt.setDate((3+(i*4)),birthdates[i]);
				pstmt.setTimestamp((4+(i*4)),lastUpdates[i]);
			}
			// Execute the prepared statement
			pstmt.executeUpdate();
			// Print confirmation message that new records were inserted.
			System.out.println("New records inserted into database.");
		}
		catch (SQLException e) {
			e.printStackTrace();
			// Print an error message if records fail to insert.
			System.out.println("Error, records failed to insert.");
		}
	}
	
	/**
	 * Delete the row where person name is 'Bob Baron'.
	 * 
	 * @return
	 * 		number of rows deleted
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public int delete() throws SQLException {
		System.out.println("Deleting a record.");
		try {
			// Create an empty statement.
			Statement stmt = con.createStatement();
			// Create the string containing the SQL command.
			String delSQL = "DELETE FROM person WHERE name='Bob Baron'";
			// Execute the SQL command in the statement.
			stmt.executeUpdate(delSQL);
			// Print a confirmation message that the record was deleted.
			System.out.println("Record deleted from table person.");
		}
		catch (SQLException e) {
			// Print error message if record failed to delete.
			System.out.println("Error, record failed to delete.");
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
	public ResultSet query1() throws SQLException {
		System.out.println("Executing query #1.");
		try {
			// Create an empty statement
			Statement stmt = con.createStatement();
			// Create the string containing the SQL command.
			String q1SQL = """
				SELECT name, salary
				FROM person
				ORDER BY salary DESC
				""";
			ResultSet q1RST = stmt.executeQuery(q1SQL);
			return q1RST;
		}
		catch (SQLException e) {
			// Print error message if query fails to execute.
			System.out.println("Error, query #1 failed to execute.");
			return null;
		}		
	}
	
	/**
	 * Query returns the person last name and salary if the person's salary is greater than the average salary of all people.
	 * 
	 * @return
	 * 		ResultSet
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public ResultSet query2() throws SQLException {
		System.out.println("Executing query #2.");
		try {
			// Create an empty statement
			Statement stmt = con.createStatement();
			// Create the string containing the SQL command.
			String q2SQL = """
				SELECT SUBSTRING_INDEX(name,' ',-1) AS lastname, salary
				FROM person
				WHERE salary>(SELECT AVG(salary) FROM person)
				""";
			ResultSet q2RST = stmt.executeQuery(q2SQL);
			return q2RST;
		}
		catch (SQLException e) {
			// Print error message if query fails to execute.
			System.out.println("Error, query #2 failed to execute.");
			return null;
		}	
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
	public ResultSet query3() throws SQLException {
		System.out.println("Executing query #3.");
		// TODO: Write SQL query
		try {
			// Create an empty statement
			Statement stmt = con.createStatement();
			// Create the string containing the SQL command.
			String q3SQL = """
				SELECT DISTINCT *
				FROM person p1
				INNER JOIN person p2
				WHERE
					(p1.id != p2.id)
					AND (HOUR(TIMEDIFF(p1.last_update,p2.last_update)) < 1)
					AND (p1.name < p2.name)
				""";
			ResultSet q3RST = stmt.executeQuery(q3SQL);
			return q3RST;
		}
		catch (SQLException e) {
			e.printStackTrace();
			// Print error message if query fails to execute.
			System.out.println("Error, query #3 failed to execute.");
			return null;
		}
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
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {                       
	if (rst == null)
		return "";
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
                
        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) { 
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
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
	    StringBuffer buf = new StringBuffer(5000);                                   
	    buf.append(meta.getColumnName(1)+" ("+meta.getColumnLabel(1)+", "+meta.getColumnType(1)+"-"+meta.getColumnTypeName(1)+", "+meta.getColumnDisplaySize(1)+", "+meta.getPrecision(1)+", "+meta.getScale(1)+")");
	    for (int j = 2; j <= meta.getColumnCount(); j++)
	        buf.append(", "+meta.getColumnName(j)+" ("+meta.getColumnLabel(j)+", "+meta.getColumnType(j)+"-"+meta.getColumnTypeName(j)+", "+meta.getColumnDisplaySize(j)+", "+meta.getPrecision(j)+", "+meta.getScale(j)+")");
	    return buf.toString();
    }
}
