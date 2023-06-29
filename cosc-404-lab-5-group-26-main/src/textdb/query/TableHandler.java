package textdb.query;

import java.io.*;
import java.sql.SQLException;
import java.util.StringTokenizer;

/**
 * Operations for accessing a table stored as a text file.
 */
public class TableHandler
{
	protected BufferedReader reader;			// Reader from console for user input
	protected RandomAccessFile raFile;			 	// File to manipulated
	protected String columnNames;				// First row of file is a tab separated list of column names
	
	
	public static void main(String[] args)
	{
		TableHandler app = new TableHandler();
		app.init();
		
		try
		{
			app.run();
		}
		catch (SQLException e)
		{
			System.out.println(e);
		}
	}

	protected void init()
	{	raFile = null;
		reader = new BufferedReader(new InputStreamReader(System.in));	// Set up console reader
	}

	private void run() throws SQLException
	{	// Continually read command from user in a loop and then do the requested query
		String choice;				
		fileCreate();
		printMenu();
		choice = getLine();
		while (!choice.equals("X") )
		{
			if (choice.equals("1"))
			{// Read in and output entire raFile using API for class RandomAccessFile
				System.out.println(readAll());
			}
			else if (choice.equals("2"))
			{//Locate and output the record with the following key
				System.out.print("Please enter the key to locate the record: ");
				String key = getLine().trim();		
				System.out.println(findRecord(key) );
			}
			else if (choice.equals("3"))
			{// Prompt for a record to insert, then append to raFile
				System.out.print("Enter the record:");
				String record = getLine().trim();
				insertRecord(record);
			}
			else if (choice.equals("4"))
			{// prompt for key of record to delete
				System.out.print("Please enter the key to locate the record to delete: ");
				String key = getLine().trim();
				deleteRecord(key);
			}
			else if (choice.equals("5"))
			{//read in key value and call update method
				System.out.print("Enter key of record to update: " );
				String key = getLine();
				System.out.print("Enter column number of field to update: ");
				String column = getLine();
				int col = Integer.parseInt(column);
				System.out.print("Enter value of field to update: ");
				String value = getLine();
				updateRecord(key, col, value);
			}
			else if (choice.equals("F"))
			{	// Specify the File to Manipulate			
				fileCreate();
			}
			else
				System.out.println("Invalid input!");

			printMenu();
			choice = getLine();
		}

		try{
				raFile.close();
		}catch(IOException io){System.out.println(io); }
	}

/****************************************************************************************
*	readAll()	reads each record in RandomAccessFile raFile and outputs each record to a String.
*				Each record should be on its own line (add a "\n" to end of each record).
*				Note: You do not have to parse each record.  Just append the whole line (make sure to trim() input).
*				Catch any IOException and re-throw it as a SQLException if any error occurs.
*************************************************************************************/
	public String readAll() throws SQLException
	{
		try{
			raFile.seek(0);
			String line = raFile.readLine();
			StringBuffer buf = new StringBuffer();
			while (line != null)
			{
				buf.append(line.trim());
				buf.append("\n");
				line = raFile.readLine();
			}
			return buf.toString();
		}catch(IOException io) { throw new SQLException("IOException in readAll: "+io); }
	}

/**************************************************************************************
*	findRecord()	takes parameter key holding the key of the record to return
*					Returns a String consisting of the columnNames string an EOL, any record if found, and an EOL.
*					If no record is found, should still return the columnNames string and an EOL.
*					Catch any IOException and re-throw it as a SQLException if any error occurs.
**************************************************************************/
	public String findRecord(String key) throws SQLException
	{
		long offset = findStartOfRecord(key);

		try{
			if(offset >= 0)
			{
				raFile.seek(offset);				
				return columnNames+"\n"+raFile.readLine().trim()+"\n";
			}
			else
				return columnNames+"\n";
		}catch(IOException io) {throw new SQLException("IOException in findRecord: "+io); }
	}

/**************************************************************************************
*	findStartOfRecord(String key)	takes parameter key holding the key of the record to find
*					 				returns the cursor position of located record
*
* Helper method to locate a record and find location of cursor
* Catch any IOException and re-throw it as a SQLException if any error occurs.
**************************************************************************/
	protected long findStartOfRecord(String key) throws SQLException
	{
		long cursor = 0;
		int keyValue = Integer.parseInt(key.trim());
		try{
			raFile.seek(0);
			String line = raFile.readLine();
			cursor = raFile.getFilePointer();
			line = raFile.readLine();			// Throw away first line as was header line
			StringTokenizer st = null;
			while( line != null )
			{
				st = new StringTokenizer(line);
				int keyLineValue = Integer.parseInt(st.nextToken().trim() );		//get first token (the key)

				if (keyValue == keyLineValue )
					return cursor;

				cursor = raFile.getFilePointer();
				line = raFile.readLine();
			}
		}catch(IOException io) { throw new SQLException("IOException in findStartOfRecord: "+io); }
		return -1;
	}

/*************************************************************************************
*	updateRecord(String key, int col, String value)
*		parameters	key - key value of record to update
*					col - column of field to update
*					value - new value for this field of the record
*		Returns the count of how many records were updated (either 0 or 1).
*
* method must find the record with key and update the field. The updated record must
* be put back in the file with all other records maintained.
* Catch any IOException and re-throw it as a SQLException if any error occurs.
************************************************************************************/
	public int updateRecord(String key, int col, String value) throws SQLException
	{
		try{
			long cursor = findStartOfRecord(key);
			if (cursor < 0)
			{								
				return 0;
			}
			else
			{
				raFile.seek(cursor);
				String record = raFile.readLine();
				long cursor2 = raFile.getFilePointer();
				//Create byte array of all data after updated record
				byte[] b = new byte[(int)raFile.length()-(int)cursor2];
				raFile.readFully(b);
				//Put cursor on record to update and write new record
				raFile.seek(cursor);
				String newLine = updateLine(record, col, value);
				raFile.setLength(raFile.length()-(cursor2-cursor) ); //truncate old data
				raFile.writeBytes(newLine.trim());
				raFile.writeBytes("\n");
				raFile.write(b);
				return 1;
			}
		}catch(Exception e){throw new SQLException("IOError in UpdateRecord: "+e); }
	}

	static protected String updateLine(String line, int field, String newValue)
	{
		StringBuffer sb = new StringBuffer(line);
		int index = -1;
		for(int i=1;i<field;i++)
		{
			index = sb.indexOf("\t",index+1);
		}
		int index2 = sb.indexOf("\t",index+1);
		if(index2<1)
			index2 = sb.length()-1;
		sb = sb.replace(index+1,index2,newValue);
		return sb.toString();
	}

/**************************************************************************************
*	deleteRecord()	takes parameter key holding the key of the record to delete
*					the method must maintain validity of entire text file
*
* Returns the count of how many records were deleted (either 0 or 1).
* Locate a record, delete the record, rewrite the rest of file to remove empty
* space of deleted record
* Catch any IOException and re-throw it as a SQLException if any error occurs.
**************************************************************************/
	public int deleteRecord(String key) throws SQLException
	{
		try{
			long cursor = findStartOfRecord(key);
			if (cursor == -1)
				return 0;			// Record not found
			
			//Create byte array to read in the rest of file
			long cursor2 = raFile.getFilePointer();
			byte[] b = new byte[(int)raFile.length()-(int)cursor2];

			raFile.readFully(b);
			raFile.seek(cursor);
			raFile.write(b);
			raFile.setLength(raFile.length()-(cursor2-cursor) ); //truncate old data
			return 1;
		}catch(IOException io){throw new SQLException("IOException in deleteRecord: "+io); }
	}


/**************************************************************************************
*	insertRecord()	Appends records to end of file.
*					the method must maintain validity of entire text file
*
*					Return 1 if record successfully inserted, 0 otherwise.
* Catch any IOException and re-throw it as a SQLException if any error occurs.
**************************************************************************/
	public int insertRecord(String record) throws SQLException
	{
		try{
			raFile.seek(raFile.length());
			raFile.writeBytes(record.trim());
			raFile.writeBytes("\n");
			return 1;
		}catch(IOException io){throw new SQLException("IOException insertRecord: "+io); }
	}



/**********************************************************************************
* The below methods read in a line of input from standard input and create a
* RandomAccessFile to manipulate.  printMenu() prints the menu to enter options
*
* The code works and should not need to be updated.
************************************************************************/
//Input method to read a line from standard input
	protected String getLine()
	{	 String inputLine = "";
		  try{
			  inputLine = reader.readLine();
		  }catch(IOException e){
			 System.out.println(e);
			 System.exit(1);
		  }//end catch
	     return inputLine;
	}
	
	protected void fileCreate()
	{
		System.out.print("Enter the file name to manipulate:");
		String fileName = getLine();
		try {
			fileCreate(fileName);
		}
		catch (SQLException e)
		{	System.out.println("Error opening file: "+fileName+". "+e); }
	}
	
	//Creates a RandomAccessFile object from text file
	public void fileCreate(String fileName) throws SQLException
	{	
		//Create a RandomAccessFile with read and write privileges
		try{
			raFile = new RandomAccessFile(fileName, "rw" );			
				
			if(raFile.length() <1)
				System.out.println("File has "+raFile.length()+" bytes. Is the file name correct?" );
						
			// Read the first row of column names
			columnNames = raFile.readLine();			
			if (columnNames != null)
				columnNames.trim();
			// Go back to start of file
			raFile.seek(0);
		}
		catch(FileNotFoundException fnf){ throw new SQLException("File not found: "+fileName); }
		catch(IOException io) {throw new SQLException(io); }
	}
	
	public void close() throws SQLException
	{
		if (raFile != null)
		{
			try { 
				raFile.close(); 
			} 
			catch (IOException e) 
			{	throw new SQLException(e); }
		}
	}
	
	protected void printMenu()
	{	System.out.println("\n\nSelect one of these options: ");
		System.out.println("  1 - Read and Output All Lines");
		System.out.println("  2 - Return Record with Key");
		System.out.println("  3 - Insert a New Record");
		System.out.println("  4 - Delete Record with Key");
		System.out.println("  5 - Update Record with Key");
		System.out.println("  F - Specify Different Data File");
		System.out.println("  X - Exit application");
		System.out.print("Your choice: ");
	}
}
