import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


// Tests creating an index and using EXPLAIN on a PostgreSQL database.
public class IndexPostgreSQL {
	// Connection to database
	private Connection con;
	
	/**
	 * Main method is only used for convenience.  Use JUnit test file to verify your answer.
	 * 
	 * @param args
	 * 		none expected
	 * @throws SQLException
	 * 		if a database error occurs
	 */
	public static void main(String[] args) throws SQLException {
		IndexPostgreSQL q = new IndexPostgreSQL();
		q.connect();	
		q.drop();
		q.create();
		q.insert(10000);	
		q.addindex1();	
		q.addindex2();			
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
		String url = "jdbc:postgresql://localhost/lab2";
		String uid = "testuser";
		String pw = "404postgrespw";
		
		System.out.println("Connecting to database.");
		// Note: Must assign connection to instance variable as well as returning it back to the caller
		try {
			// Get connection with the provided URL, username, and password.
			con = DriverManager.getConnection(url,uid,pw);
			// Print a message for a successful connection.
			System.out.println("Connection to database established.");
		} 
		catch (SQLException e) {
			e.printStackTrace();
			// Print an error message if connection fails.
			System.out.println("Error, connection failed to establish.");
		}
		return con;	                       
	}
	
	// Closes connection to database.
	public void close() {
		System.out.println("Closing database connection.");
		try {
			// Close the connection.
			con.close();
			// Print a message for a successful close.
			System.out.println("Connection successfully closed.");
		} 
		catch (SQLException e) {
			System.out.println("Error, connection failed to close.");
		} 
	}
	
	// Drops the table from the database.  If table does not exist, error is ignored.
	public void drop() {
		System.out.println("Dropping table bench.");
		try {
			// Create an empty statement.
			Statement stmt = con.createStatement();
			// Execute the DROP TABLE command on table bench
			stmt.execute("DROP TABLE bench;");
			// Print a confirmation message that table bench was dropped.
			System.out.println("Table bench dropped.");
		}
		catch (SQLException e) {
			// Print an error message if table bench failed to drop.
			System.out.println("Error, table bench not dropped.");
		}
	}
	
	/**
	 * Creates the table in the database.  Table name: bench
	 * Fields:
	 *  - id - integer, must auto-increment
	 *  - val1 - integer (starts at 1 and each record increases by 1)
	 *  - val2 - integer (val1 % 10)
	 *  - str1 - varchar(20) = "Test"+val1
	 * Note: You do NOT need to use calculated or derived fields.
	 */
	public void create() throws SQLException {
		System.out.println("Creating table bench.");
		try {
			// Create an empty statement.
			Statement stmt = con.createStatement();
			// Create the string containing the SQL command
			String createSQL = """
				CREATE TABLE bench 
				(id SERIAL PRIMARY KEY, 
				val1 INTEGER, 
				val2 INTEGER, 
				str1 VARCHAR(20));
				""";
			// Execute the SQL command in the statement.
			stmt.executeUpdate(createSQL);
			// Print a confirmation message that table person was created.
			System.out.println("Table bench created in database.");
		}
		catch (SQLException e) {
			e.printStackTrace();
			// Print an error message if table bench failed to be created.
			System.out.println("Error, table bench not created.");
		}			
	}
	
	// Inserts the test records in the database.  Must used a PreparedStatement.
	public void insert(int numRecords) throws SQLException {
		System.out.println("Inserting records.");
		try {
			// Create a string containing the INSERT command without any values
			String insSQL = """
				INSERT INTO bench (val1,val2,str1)
				VALUES
				""";
			// Append a number of record placeholders as provided by numRecords
			for(int i = 0;i < numRecords;i++) {
				if(i == (numRecords-1)) {
					insSQL += "(?,?,?);";
				}else {
					insSQL += "(?,?,?),\n";
				}
			}
			// Create the prepared statement with the command to insert a number of records as provided by numRecords.
			PreparedStatement pstmt = con.prepareStatement(insSQL);
			// Loop through the number of records needing to be added and insert the various values into their positions
			for(int i = 0; i < numRecords;i++) {
				pstmt.setInt(1+(i*3),(i+1));
				pstmt.setInt(2+(i*3),((i+1)%10));
				pstmt.setString(3+(i*3),"Test"+(i+1));
			}
			// Execute the prepared statement
			pstmt.executeUpdate();
			// Print confirmation message that new records were inserted.
			System.out.println("New records inserted into database.");
		}
		catch (SQLException e) {
			// Print an error message if records fail to insert.
			System.out.println("Error, records failed to insert.");
		}	
	}
	
	/**
	 * Creates a unique index on val1 for bench table.  Returns result of explain.
	 * 
	 * @return
	 * 		ResultSet
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public ResultSet addindex1() throws SQLException {
		System.out.println("Building index #1.");
		try {
			// Create an empty statement.
			Statement indexSTMT = con.createStatement();
			// Create a String containing the SQL command.
			String addIndex1 = "CREATE UNIQUE INDEX idxBenchVal1 ON bench(val1);";
			// Execute the statement with the SQL command
			indexSTMT.executeUpdate(addIndex1);
			// Print a confirmation message once index is create.
			System.out.println("Index #1 built.");
		}
		catch (SQLException e) {
			// Print an error message if the index fails to create.
			System.out.println("Error, index #1 not built.");
		}
		try {
			// Create an empty statement.
			Statement querySTMT = con.createStatement();
			// Create a String containing the SQL command.
			String addIndex1Query = "EXPLAIN(SELECT * FROM bench WHERE val1 = 500)";
			// Execute the statement with the SQL command and save into a ResultSet.
			ResultSet index1RST = querySTMT.executeQuery(addIndex1Query);
			// Print a confirmation message that the ResultSet was created.
			System.out.println("Query #1 executed, ResultSet created.");
			// Return the ResultSet
			return index1RST;
		}
		catch (SQLException e) {
			// Print an error message if the query fails to execute.
			System.out.println("Error, index #1 query failed to executed.");
			// Return a null ResultSet
			return null;
		}	
	}
	
	/**
	 * Creates an index on val2 and val1 for bench table.  Returns result of explain.
	 * 
	 * @return
	 * 		ResultSet
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public ResultSet addindex2() throws SQLException
	{
		System.out.println("Building index #2.");
		try {
			// Create an empty statement.
			Statement indexSTMT = con.createStatement();
			// Create a String containing the SQL command.
			String addIndex2 = "CREATE INDEX idxBenchVal2Val1 ON bench(val2,val1);";
			// Execute the statement with the SQL command
			indexSTMT.executeUpdate(addIndex2);
			// Print a confirmation message once index is create.
			System.out.println("Index #2 built.");
		}
		catch (SQLException e) {
			// Print an error message if the index fails to create.
			System.out.println("Error, index #2 not built.");
		}
		try {
			// Create an empty statement.
			Statement querySTMT = con.createStatement();
			// Create a String containing the SQL command.
			String addIndex2Query = "EXPLAIN(SELECT * FROM bench WHERE val2 = 0 and val1 > 100)";
			// Execute the statement with the SQL command and save into a ResultSet.
			ResultSet index2RST = querySTMT.executeQuery(addIndex2Query);
			// Print a confirmation message that the ResultSet was created.
			System.out.println("Query #2 executed, ResultSet created.");
			// Return the ResultSet
			return index2RST;
		}
		catch (SQLException e) {
			// Print an error message if the query fails to execute.
			System.out.println("Error, index #2 query failed to executed.");
			// Return a null ResultSet
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
