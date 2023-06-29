package textdb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;
import java.util.StringTokenizer;

public class TextDBResultSet implements ResultSet {

	private ArrayList<String> results;
	private int currentLoc;
	private String[] columns;					// The value of the columns for the current row
	private String[] columnNames;				// Names of each column (should be first row in file)
	private TextDBResultSetMetaData metadata;	// Metadata description of ResultSet
	
	public TextDBResultSet(String result)
	{
		// TODO: Complete this method.
		
		// Parse all results into an ArrayList for easier retrieval
		// Initialize result 		
		results = new ArrayList<>();
		//use a tokenize to split the String from \n
		StringTokenizer tokenizedString = new StringTokenizer(result, "\n");
		// Place the TokenizedString data into arraylist
		while (tokenizedString.hasMoreTokens()) {
			// each token still has tabs, thus the token needs to be broken down further
			String[] prepepingToken = tokenizedString.nextToken().toString().split("	");
			// adding each value into teh arraylist
			for (int i = 0; i < prepepingToken.length; i++) {
				results.add(prepepingToken[i]);
			}
			
		}

		// First row is the row of column names
		columnNames = new String[4];
		// Adding the names of columns into columnNames
		for (int i = 0; i < columnNames.length; i++) {
			columnNames[i] = results.get(i);
		}

		// Set up result rows
		columns = new String[4];
		// Adding the values of each record into columns
		for (int j = 0; j < columns.length && results.size()>8; j++) {
			columns[j] = results.get(j+4);
		}
		
		// Initialize currentLoc and metadata
		this.currentLoc = 0;
		this.metadata = new TextDBResultSetMetaData(columnNames);
		
	}
	
	@Override
	public boolean next() throws SQLException {		
		// TODO: Complete this method.
		// Advance to next row				
		
		// because next has be called 	
		currentLoc = currentLoc+1;

		// Parse the column contents into array columns
		// determining if the current location is still within the resultset, if not return false because the point has went beyond the token
		if(currentLoc < this.results.size()/4) {
			int index = 4*currentLoc;
			for (int j = 0; j < columns.length; j++) {
				columns[j] = results.get(j+index);
			}
			return true;
		} else {
		return false;
		}
	}

	
	@Override
	public int getInt(int idx) throws SQLException {
		if (idx < 1 || idx > columnNames.length)
			throw new SQLException("Invalid column index.");
		
		return Integer.parseInt(columns[idx-1]);		
	}

	@Override
	public int getInt(String name) throws SQLException {
		int col = findColumn(name);
		return getInt(col-1);
	}
	
	@Override
	public Object getObject(int idx) throws SQLException {
		// TODO: Complete this method.
		// return the object
		return columns[idx-1];	
	}

	@Override
	public Object getObject(String name) throws SQLException {
		// TODO: Complete this method.
		// reutn the object
		int col = findColumn(name);
		return columns[col-1];	
	}
	
	@Override
	public String getString(int idx) throws SQLException {
		if (idx < 1 || idx > columnNames.length)
			throw new SQLException("Invalid column index.");
		
		return columns[idx-1];	
	}

	@Override
	public String getString(String name) throws SQLException {
		int col = findColumn(name);
		return getString(col-1);
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}
	
	/**
	 * Returns column index (starting at 1) if column name is found.  Throws an SQLException otherwise.
	 * @param name
	 * @return
	 */
	public int findColumn(String name) throws SQLException
	{
		for (int i=0; i < columnNames.length; i++)
		{	if (name.equalsIgnoreCase(columnNames[i]))
				return i;
		}
		throw new SQLException("Column name: "+name+" not found.");
	}
	
	
	
	/*
	 * Do not need to edit any methods below this point.
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {		
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {		
		return null;
	}

	@Override
	public boolean absolute(int arg0) throws SQLException {		
		return false;
	}

	@Override
	public void afterLast() throws SQLException {
	}

	@Override
	public void beforeFirst() throws SQLException {

	}

	@Override
	public void cancelRowUpdates() throws SQLException {
	

	}

	@Override
	public void clearWarnings() throws SQLException {


	}

	@Override
	public void close() throws SQLException {
	

	}

	@Override
	public void deleteRow() throws SQLException {
	

	}	

	@Override
	public boolean first() throws SQLException {
	
		return false;
	}

	@Override
	public Array getArray(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Array getArray(String arg0) throws SQLException {
	
		return null;
	}

	@Override
	public InputStream getAsciiStream(int arg0) throws SQLException {

		return null;
	}

	@Override
	public InputStream getAsciiStream(String arg0) throws SQLException {
	
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int arg0) throws SQLException {
	
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String arg0) throws SQLException {

		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
	
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
	
		return null;
	}

	@Override
	public InputStream getBinaryStream(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public InputStream getBinaryStream(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Blob getBlob(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Blob getBlob(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public boolean getBoolean(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public boolean getBoolean(String arg0) throws SQLException {
		
		return false;
	}

	@Override
	public byte getByte(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public byte getByte(String arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public byte[] getBytes(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public byte[] getBytes(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Reader getCharacterStream(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Reader getCharacterStream(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Clob getClob(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Clob getClob(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public int getConcurrency() throws SQLException {
		
		return 0;
	}

	@Override
	public String getCursorName() throws SQLException {
		
		return null;
	}

	@Override
	public Date getDate(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Date getDate(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Date getDate(int arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Date getDate(String arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public double getDouble(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public double getDouble(String arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		
		return 0;
	}

	@Override
	public int getFetchSize() throws SQLException {
		
		return 0;
	}

	@Override
	public float getFloat(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public float getFloat(String arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public int getHoldability() throws SQLException {
		
		return 0;
	}

	@Override
	public long getLong(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public long getLong(String arg0) throws SQLException {
		
		return 0;
	}	

	@Override
	public Reader getNCharacterStream(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Reader getNCharacterStream(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public NClob getNClob(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public NClob getNClob(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public String getNString(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public String getNString(String arg0) throws SQLException {
		
		return null;
	}

	

	@Override
	public Object getObject(int arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public Object getObject(String arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public Ref getRef(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Ref getRef(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public int getRow() throws SQLException {
		
		return 0;
	}

	@Override
	public RowId getRowId(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public RowId getRowId(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public SQLXML getSQLXML(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public SQLXML getSQLXML(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public short getShort(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public short getShort(String arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public Statement getStatement() throws SQLException {
		
		return null;
	}

	

	@Override
	public Time getTime(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Time getTime(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Time getTime(int arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Time getTime(String arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(String arg0, Calendar arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public int getType() throws SQLException {
		
		return 0;
	}

	@Override
	public URL getURL(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public URL getURL(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public InputStream getUnicodeStream(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public InputStream getUnicodeStream(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		
		return null;
	}

	@Override
	public void insertRow() throws SQLException {
		

	}

	@Override
	public boolean isAfterLast() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isClosed() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		
		return false;
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		

	}

	@Override
	public void moveToInsertRow() throws SQLException {
		

	}

	
	@Override
	public boolean previous() throws SQLException {
		
		return false;
	}

	@Override
	public void refreshRow() throws SQLException {
		

	}

	@Override
	public boolean relative(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		
		return false;
	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException {
		

	}

	@Override
	public void setFetchSize(int arg0) throws SQLException {
		

	}

	@Override
	public void updateArray(int arg0, Array arg1) throws SQLException {
		

	}

	@Override
	public void updateArray(String arg0, Array arg1) throws SQLException {
		

	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1)
			throws SQLException {
		

	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1)
			throws SQLException {
		

	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		

	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1, int arg2)
			throws SQLException {
		

	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		

	}

	@Override
	public void updateBigDecimal(String arg0, BigDecimal arg1)
			throws SQLException {
		

	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1)
			throws SQLException {
		

	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1)
			throws SQLException {
		

	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		

	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1, int arg2)
			throws SQLException {
		

	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateBlob(int arg0, Blob arg1) throws SQLException {
		

	}

	@Override
	public void updateBlob(String arg0, Blob arg1) throws SQLException {
		

	}

	@Override
	public void updateBlob(int arg0, InputStream arg1) throws SQLException {
		

	}

	@Override
	public void updateBlob(String arg0, InputStream arg1) throws SQLException {
		

	}

	@Override
	public void updateBlob(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateBlob(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateBoolean(int arg0, boolean arg1) throws SQLException {
		

	}

	@Override
	public void updateBoolean(String arg0, boolean arg1) throws SQLException {
		

	}

	@Override
	public void updateByte(int arg0, byte arg1) throws SQLException {
		

	}

	@Override
	public void updateByte(String arg0, byte arg1) throws SQLException {
		

	}

	@Override
	public void updateBytes(int arg0, byte[] arg1) throws SQLException {
		

	}

	@Override
	public void updateBytes(String arg0, byte[] arg1) throws SQLException {
		

	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1)
			throws SQLException {
		

	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1)
			throws SQLException {
		

	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1, int arg2)
			throws SQLException {
		

	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1, int arg2)
			throws SQLException {
		

	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateClob(int arg0, Clob arg1) throws SQLException {
		

	}

	@Override
	public void updateClob(String arg0, Clob arg1) throws SQLException {
		

	}

	@Override
	public void updateClob(int arg0, Reader arg1) throws SQLException {
		

	}

	@Override
	public void updateClob(String arg0, Reader arg1) throws SQLException {
		

	}

	@Override
	public void updateClob(int arg0, Reader arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateClob(String arg0, Reader arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateDate(int arg0, Date arg1) throws SQLException {
		

	}

	@Override
	public void updateDate(String arg0, Date arg1) throws SQLException {
		

	}

	@Override
	public void updateDouble(int arg0, double arg1) throws SQLException {
		

	}

	@Override
	public void updateDouble(String arg0, double arg1) throws SQLException {
		

	}

	@Override
	public void updateFloat(int arg0, float arg1) throws SQLException {
		

	}

	@Override
	public void updateFloat(String arg0, float arg1) throws SQLException {
		

	}

	@Override
	public void updateInt(int arg0, int arg1) throws SQLException {
		

	}

	@Override
	public void updateInt(String arg0, int arg1) throws SQLException {
		

	}

	@Override
	public void updateLong(int arg0, long arg1) throws SQLException {
		

	}

	@Override
	public void updateLong(String arg0, long arg1) throws SQLException {
		

	}

	@Override
	public void updateNCharacterStream(int arg0, Reader arg1)
			throws SQLException {
		

	}

	@Override
	public void updateNCharacterStream(String arg0, Reader arg1)
			throws SQLException {
		

	}

	@Override
	public void updateNCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateNCharacterStream(String arg0, Reader arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateNClob(int arg0, NClob arg1) throws SQLException {
		

	}

	@Override
	public void updateNClob(String arg0, NClob arg1) throws SQLException {
		

	}

	@Override
	public void updateNClob(int arg0, Reader arg1) throws SQLException {
		

	}

	@Override
	public void updateNClob(String arg0, Reader arg1) throws SQLException {
		

	}

	@Override
	public void updateNClob(int arg0, Reader arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateNClob(String arg0, Reader arg1, long arg2)
			throws SQLException {
		

	}

	@Override
	public void updateNString(int arg0, String arg1) throws SQLException {
		

	}

	@Override
	public void updateNString(String arg0, String arg1) throws SQLException {
		

	}

	@Override
	public void updateNull(int arg0) throws SQLException {
		

	}

	@Override
	public void updateNull(String arg0) throws SQLException {
		

	}

	@Override
	public void updateObject(int arg0, Object arg1) throws SQLException {
		

	}

	@Override
	public void updateObject(String arg0, Object arg1) throws SQLException {
		

	}

	@Override
	public void updateObject(int arg0, Object arg1, int arg2)
			throws SQLException {
		

	}

	@Override
	public void updateObject(String arg0, Object arg1, int arg2)
			throws SQLException {
		

	}

	@Override
	public void updateRef(int arg0, Ref arg1) throws SQLException {
		

	}

	@Override
	public void updateRef(String arg0, Ref arg1) throws SQLException {
		

	}

	@Override
	public void updateRow() throws SQLException {
		

	}

	@Override
	public void updateRowId(int arg0, RowId arg1) throws SQLException {
		

	}

	@Override
	public void updateRowId(String arg0, RowId arg1) throws SQLException {
		

	}

	@Override
	public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
		

	}

	@Override
	public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
		

	}

	@Override
	public void updateShort(int arg0, short arg1) throws SQLException {
		

	}

	@Override
	public void updateShort(String arg0, short arg1) throws SQLException {
		

	}

	@Override
	public void updateString(int arg0, String arg1) throws SQLException {
		

	}

	@Override
	public void updateString(String arg0, String arg1) throws SQLException {
		

	}

	@Override
	public void updateTime(int arg0, Time arg1) throws SQLException {
		

	}

	@Override
	public void updateTime(String arg0, Time arg1) throws SQLException {
		

	}

	@Override
	public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
		

	}

	@Override
	public void updateTimestamp(String arg0, Timestamp arg1)
			throws SQLException {
		

	}

	@Override
	public boolean wasNull() throws SQLException {
		
		return false;
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		
		return null;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		
		return null;
	}

}
