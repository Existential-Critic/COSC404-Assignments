package textdb;

/**
TableHandler.java - A java program for accessing and updating a database table stored as a tab
separated text file.  Each record is stored on a new line. Implement the following methods associated
with a menu item. The run() method will prompt for the input required for execution of these methods

1 - void readAll()
2 - String findRecord(String key )
3 - void insertRecord(String record)
4 - void deleteRecord(String key)
5 - void updateRecord(String key, int col, String value)

Helper method - long findStartOfRecord(String key) to find byte offset from start of file for start of record.
*/

import java.io.*;
import java.sql.SQLException;
import java.util.Arrays;


public class TableHandler {
	private BufferedReader reader;			// Reader from console for user input
	RandomAccessFile raFile;			 	// File to manipulated
	private String columnNames;				// First row of file is a tab separated list of column names

	public static void main(String[] args) {
		TableHandler app = new TableHandler();
		app.init();
		
		try {
			app.run();
		}
		catch (SQLException e) {
			System.out.println(e);
		}
	}

	private void init() {	raFile = null;
		reader = new BufferedReader(new InputStreamReader(System.in));	// Set up console reader
	}

	private void run() throws SQLException {	// Continually read command from user in a loop and then do the requested query
		String choice;				
		fileCreate();
		printMenu();
		choice = getLine();
		while (!choice.equals("X") ) {
			if (choice.equals("1")) {	// Read in and output entire raFile using API for class RandomAccessFile
				System.out.println(readAll());
			}
			else if (choice.equals("2")) {	//Locate and output the record with the following key
				System.out.print("Please enter the key to locate the record: ");
				String key = getLine().trim();		
				System.out.println(findRecord(key) );
			}
			else if (choice.equals("3")) {	// Prompt for a record to insert, then append to raFile
				System.out.print("Enter the record:");
				String record = getLine().trim();
				insertRecord(record);
			}
			else if (choice.equals("4")) {	// prompt for key of record to delete
				System.out.print("Please enter the key to locate the record to delete: ");
				String key = getLine().trim();
				deleteRecord(key);
			}
			else if (choice.equals("5")) {	//read in key value and call update method
				System.out.print("Enter key of record to update: " );
				String key = getLine();
				System.out.print("Enter column number of field to update: ");
				String column = getLine();
				int col = Integer.parseInt(column);
				System.out.print("Enter value of field to update: ");
				String value = getLine();
				updateRecord(key, col, value);
			}
			else if (choice.equals("F")) {	// Specify the File to Manipulate			
				fileCreate();
			}
			else
				System.out.println("Invalid input!");

			printMenu();
			choice = getLine();
		}

		try {
				raFile.close();
		}
		catch(IOException io){
			System.out.println(io);
		}
	}

/****************************************************************************************
*	readAll()	reads each record in RandomAccessFile raFile and outputs each record to a String.
*				Each record should be on its own line (add a "\n" to end of each record).
*				Note: You do not have to parse each record.  Just append the whole line (make sure to trim() input).
*				Catch any IOException and re-throw it as a SQLException if any error occurs.
*************************************************************************************/
	public String readAll() throws SQLException {
		try {
			// Reset pointer to the beginning of file.
			raFile.seek(0);
			// Initialise an empty string to return.
			String fullRecord = "";
			// Initialise a null string.
			String precomp;
			// Iterate through file, saving the current line in precomp so we do not skip lines.
			while((precomp = raFile.readLine()) != null) {
				// Append the fullRecord string with the read line and a new line.
				fullRecord += (precomp.trim() + "\n");
			}
			// Return the full string.
			return fullRecord;
		}
		catch (IOException e) {
			// Catch any IOException and rethrow as SQL exception.
			throw new SQLException(e);
		}
	}

/**************************************************************************************
*	findRecord()	takes parameter key holding the key of the record to return
*					Returns a String consisting of the columnNames string an EOL, any record if found, and an EOL.
*					If no record is found, should still return the columnNames string and an EOL.
*					Catch any IOException and re-throw it as a SQLException if any error occurs.
**************************************************************************/
	public String findRecord(String key) throws SQLException {
		try {
			// Reset pointer to the beginning of file.
			raFile.seek(0);
			// Initialise a string that only contains the column names.
			String foundRecord = raFile.readLine().trim() + "\n";
			// Call the helper methd to find the start of the selected record.
			raFile.seek(findStartOfRecord(key));
			// If the returned location is not the beginning of file (IE, record does not exist), append string with the record and a new line.
			if(raFile.getFilePointer() != 0) {
				foundRecord += (raFile.readLine().trim() + "\n");
			}
			// Return the found record.
			return foundRecord;
		}
		catch (IOException e) {
			// Catch any IOException and rethrow as SQL exception.
			throw new SQLException(e);
		}
	}

/**************************************************************************************
*	findStartOfRecord(String key)	takes parameter key holding the key of the record to find
*					 				returns the cursor position of located record
*
* Helper method to locate a record and find location of cursor
* Catch any IOException and re-throw it as a SQLException if any error occurs.
**************************************************************************/
	private long findStartOfRecord(String key) throws SQLException {
		try {
			// Reset pointer to the beginning of file.
			raFile.seek(0);
			// Initialise long as 0. If it remains zero, it means the record does not exist.
			long byteLocation = 0;
			// Initialise a null string.
			String precomp;
			// Iterate through file, saving the current line in precomp so we do not skip lines.
			while((precomp = raFile.readLine()) != null) {
				// For each read line, split on tabs into an array of strings.
				String[] precompSplit = precomp.trim().split("\t");
				// Compare the provided key against the ID of the record.
				if(key.equals(precompSplit[0])) {
					// If key and ID match, return byte location.
					return byteLocation;
				}
				// Update byteLocation with the current pointer.
				byteLocation = raFile.getFilePointer();
			}
			// Return 0 if record is not found.
			return 0;	
		}
		catch (IOException e) {
			// Catch any IOException and rethrow as SQL exception.
			throw new SQLException(e);
		}
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
	public int updateRecord(String key, int col, String value) throws SQLException {
		try {
			// Reset pointer to the beginning of file.
			raFile.seek(0);
			// Initialise an empty string array to contain the entire record.
			String[] fullRecord = {};
			// Initialise a null string.
			String precomp;
			// Iterate through file, saving the current line in precomp so we do not skip lines.
			while((precomp = raFile.readLine()) != null) {
				// Append the fullRecord string array with the read line.
				fullRecord = Arrays.copyOf(fullRecord, fullRecord.length+1);
				fullRecord[fullRecord.length-1] = precomp;
			}
			System.out.println(fullRecord.length);
			// Iterate through each saved record and find the record which the key references
			for(int i = 0;i < fullRecord.length;i++) {
				// Split the saved record into an array where the ID is in the first index.
				String[] splitRecord = fullRecord[i].split("\t");
				if(key.equals(splitRecord[0])) {
					// If key and ID match, replace the element in the given column with the provided new value.
					splitRecord[col-1] = value;
					// Rejoin the split record.
					String newRecord = String.join("\t",splitRecord);
					// Replace the existing element of the full record with the new updated record.
					fullRecord[i] = newRecord;
					// Clear raFile.
					raFile.setLength(0);
					// Iterate through the updated record and write each line, adding a new line after each string.
					for(int j = 0;j < fullRecord.length;j++) {
						raFile.writeBytes((fullRecord[j] + "\n"));
					}
					// Return 1 to show a record was deleted.
					return 1;
				}
			}
			// Return 0 if no records were updated.
			return 0;
		}
		catch (IOException e) {
			// Catch any IOException and rethrow as SQL exception.
			throw new SQLException(e);
		}
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
	public int deleteRecord(String key) throws SQLException {
		try {
			// Reset pointer to the beginning of file.
			raFile.seek(0);
			// Initialise an empty string array to contain the entire record.
			String[] fullRecord = {};
			// Initialise a null string.
			String precomp;
			// Iterate through file, saving the current line in precomp so we do not skip lines.
			while((precomp = raFile.readLine()) != null) {
				// Append the fullRecord string array with the read line.
				fullRecord = Arrays.copyOf(fullRecord, fullRecord.length+1);
				fullRecord[fullRecord.length-1] = precomp;
			}
			// Iterate through each saved record and find the record which the key references
			for(int i = 0;i < fullRecord.length;i++) {
				// Split the saved record into an array where the ID is in the first index.
				String[] splitRecord = fullRecord[i].split("\t");
				// Compare the provided key against the ID of the record.
				if(key.equals(splitRecord[0])) {
					// If key and ID match, create a new array that does not contain that index that is one size smaller.
					String[] newRecord = new String[fullRecord.length-1];
					// Iterate through original full record and copy each record except the key record.
					for(int j = 0, k = 0; j < fullRecord.length;j++) {
						// If j = i, skip that record
						if(j == i) {
							continue;
						}
						// Copy records to new array
						newRecord[k++] = fullRecord[j];
					}
					// Clear raFile.
					raFile.setLength(0);
					// Iterate through the new record and write each line, adding a new line after each string.
					for(int j = 0;j < newRecord.length;j++) {
						raFile.writeBytes((newRecord[j] + "\n"));
					}
					// Return 1 to show a record was deleted.
					return 1;
				}
			}
			// Return 0 if no records were deleted.
			return 0;
		}
		catch (IOException e) {
			// Catch any IOException and rethrow as SQL exception.
			throw new SQLException(e);
		}
	}


/**************************************************************************************
*	insertRecord()	Appends records to end of file.
*					the method must maintain validity of entire text file
*
*					Return 1 if record successfully inserted, 0 otherwise.
* Catch any IOException and re-throw it as a SQLException if any error occurs.
**************************************************************************/
	public int insertRecord(String record) throws SQLException {
		try {
			// Reset pointer to the beginning of file.
			raFile.seek(0);
			// Initialise an empty string array to contain the entire record.
			String[] fullRecord = {};
			// Initialise a null string.
			String precomp;
			// Iterate through file, saving the current line in precomp so we do not skip lines.
			while((precomp = raFile.readLine()) != null) {
				// Append the fullRecord string array with the read line.
				fullRecord = Arrays.copyOf(fullRecord, fullRecord.length+1);
				fullRecord[fullRecord.length-1] = precomp;
			}
			// Split the provided record to get the ID to compare to the existing records.
			String newRecordID = record.split("\t")[0];
			// Iterate through the existing records and compare their IDs against the new record ID.
			for(int i = 0;i < fullRecord.length;i++) {
				String fullRecordID = fullRecord[i].split("\t")[0];
				if(newRecordID.equals(fullRecordID)) {
					return 0;
				}
			}
			// Append the fullRecord string array with the new record and a new line.
			fullRecord = Arrays.copyOf(fullRecord, fullRecord.length+1);
			fullRecord[fullRecord.length-1] = record;
			// Clear raFile.
			raFile.setLength(0);
			// Iterate through the new record and write each line, adding a new line after each string.
			for(int i = 0;i < fullRecord.length;i++) {
				raFile.writeBytes((fullRecord[i]+ "\n"));
			}
			return 1;
		}
		catch (IOException e) {
			// Catch any IOException and rethrow as SQL exception.
			throw new SQLException(e);
		}
	}



/**********************************************************************************
* The below methods read in a line of input from standard input and create a
* RandomAccessFile to manipulate.  printMenu() prints the menu to enter options
*
* The code works and should not need to be updated.
************************************************************************/
//Input method to read a line from standard input
	private String getLine() {
		String inputLine = "";
		try{
			inputLine = reader.readLine();
		}
		catch(IOException e) {
			System.out.println(e);
			System.exit(1);
		}//end catch
	    return inputLine;
	}
	
	private void fileCreate() {
		System.out.print("Enter the file name to manipulate:");
		String fileName = getLine();
		try {
			fileCreate(fileName);
		}
		catch (SQLException e) {
			System.out.println("Error opening file: "+fileName+". "+e); }
	}
	
	//Creates a RandomAccessFile object from text file
	public void fileCreate(String fileName) throws SQLException {	
		//Create a RandomAccessFile with read and write privileges
		try {
			raFile = new RandomAccessFile(fileName, "rw" );
			if(raFile.length() <1)
				System.out.println("File has "+raFile.length()+" bytes. Is the file name correct?" );
			// Read the first row of column names
			columnNames = raFile.readLine().trim();
			// Go back to start of file
			raFile.seek(0);
		}
		catch(FileNotFoundException fnf){ throw new SQLException("File not found: "+fileName); }
		catch(IOException io) {throw new SQLException(io); }
	}
	
	public void close() throws SQLException {
		if (raFile != null) {
			try { 
				raFile.close(); 
			} 
			catch (IOException e) {	throw new SQLException(e); }
		}
	}
	
	private void printMenu() {	System.out.println("\n\nSelect one of these options: ");
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
