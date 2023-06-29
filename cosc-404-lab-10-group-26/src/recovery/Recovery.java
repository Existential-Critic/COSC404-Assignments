package recovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import recovery.LogRecord.LogRecordType;

// Reads a write-ahead log and uses it to restore the database into a consistent state.
public class Recovery {

	private static String filePath = "bin/data/";

	/**
	 * Main method
	 * 
	 * @param args
	 *             no arguments required
	 */
	public static void main(String[] args) throws Exception {
		Recovery recovery = new Recovery();

		recovery.recover(filePath+"log1.csv");
		recovery.recover(filePath+"log2.csv");
		recovery.recover(filePath+"log3.csv");
		recovery.recover(filePath+"log4.csv");
	}

	/**
	 * Given a write-ahead log, returns the database after recovery.
	 * 
	 * @return
	 *         connection
	 */
	public Database recover(String fileName) {
		System.out.println("\nPerforming recovery on WAL: " + fileName);

		Database db = new Database();

		// Read the log records into an ArrayList from a file
		ArrayList<LogRecord> records = readLog(fileName);

		// Create UNDO and REDO lists
		HashSet<String> undoList = new HashSet<String>();
		HashSet<String> redoList = new HashSet<String>();

		// Initialise start/end values
		int p1Start = 0;
		int p1End = records.size()-1;
		// Initialise check boolean
		boolean checkEndFound = false;
		// Initialise booleans of committed Transactions
		boolean t1Commit = false;
		boolean t2Commit = false;
		boolean t3Commit = false;
		boolean t4Commit = false;
		boolean t5Commit = false;
		// Pass #1: Determine undo and redo lists.
		// Start scan at end of log and go towards front of log until hit start of log or checkpoint start with matching checkpoint end.
		for(int i = records.size()-1;i >= 0;i--) {
			LogRecord currRecord = records.get(i);
			if(currRecord.getType().toString().equals("CHECKPOINT_START")) {
				redoList.add(currRecord.toString());
				if(checkEndFound) {
					p1Start = i;
					checkEndFound = false;
				}
			} else if(currRecord.getType().toString().equals("CHECKPOINT_END")) { // TODO: Put all data values into database. Comma-separate key value pairs
				redoList.add(currRecord.toString());
				checkEndFound = true;
				String[] endValues = currRecord.getTransaction().split(",");
				for(int j = 0;j < endValues.length;j+=2) {
					if(endValues[j].equals("A")) {
						db.put("A",Integer.parseInt(endValues[j+1]));
					}
					if(endValues[j].equals("B")) {
						db.put("B",Integer.parseInt(endValues[j+1]));
					}
					if(endValues[j].equals("C")) {
						db.put("C",Integer.parseInt(endValues[j+1]));
					}
					if(endValues[j].equals("D")) {
						db.put("D",Integer.parseInt(endValues[j+1]));
					}
					if(endValues[j].equals("E")) {
						db.put("E",Integer.parseInt(endValues[j+1]));
					}
				}
			} else { // Check whether to put into redo or undo list
				// Check if booleans need to be updated
				if(currRecord.getTransaction().toString().equals("T1") && currRecord.getType().toString().equals("COMMIT")) {
					t1Commit = true;
				} else if(currRecord.getTransaction().toString().equals("T2") && currRecord.getType().toString().equals("COMMIT")) {
					t2Commit = true;
				} else if(currRecord.getTransaction().toString().equals("T3") && currRecord.getType().toString().equals("COMMIT")) {
					t3Commit = true;
				} else if(currRecord.getTransaction().toString().equals("T4") && currRecord.getType().toString().equals("COMMIT")) {
					t4Commit = true;
				} else if(currRecord.getTransaction().toString().equals("T5") && currRecord.getType().toString().equals("COMMIT")) {
					t5Commit = true;
				}

				// Check which list to put item into
				if(currRecord.getTransaction().toString().equals("T1")) {
					if(t1Commit) {
						redoList.add(currRecord.toString());
					}else {
						undoList.add(currRecord.toString());
					}
				} else if(currRecord.getTransaction().toString().equals("T2")) {
					if(t2Commit) {
						redoList.add(currRecord.toString());
					}else {
						undoList.add(currRecord.toString());
					}
				} else if(currRecord.getTransaction().toString().equals("T3")) {
					if(t3Commit) {
						redoList.add(currRecord.toString());
					}else {
						undoList.add(currRecord.toString());
					}
				} else if(currRecord.getTransaction().toString().equals("T4")) {
					if(t4Commit) {
						redoList.add(currRecord.toString());
					}else {
						undoList.add(currRecord.toString());
					}
				} else if(currRecord.getTransaction().toString().equals("T5")) {
					if(t5Commit) {
						redoList.add(currRecord.toString());
					}else {
						undoList.add(currRecord.toString());
					}
				}
			}
												 
		}
		// Record start and end of pass #1    	
		db.setEndPass(1,p1Start);
		db.setStartPass(1,p1End);

		// REDO PASS #2
		// Initialise start/end values
		int p2Start = p1Start;
		int p2End = p1Start;
		// Pass #2: REDO from start of log (or CHECKPOINT START with matching CHECKPOINT END) until have redone all operations for transactions in redo list
		for(int i = p2Start;i < records.size();i++) {
			LogRecord currRecord = records.get(i);
			if(redoList.contains(currRecord.toString())) {
				p2End = i;
				if(currRecord.getItem() != null) {
					if(currRecord.getItem().equals("A")) {
						db.put("A",currRecord.getUpdatedValue());
					}
					if(currRecord.getItem().equals("B")) {
						db.put("B",currRecord.getUpdatedValue());
					}
					if(currRecord.getItem().equals("C")) {
						db.put("C",currRecord.getUpdatedValue());
					}
					if(currRecord.getItem().equals("D")) {
						db.put("D",currRecord.getUpdatedValue());
					}
					if(currRecord.getItem().equals("E")) {
						db.put("E",currRecord.getUpdatedValue());
					}
				}
			}
		}
		// Record start and end of pass #2
		db.setStartPass(2,p2Start);
		db.setEndPass(2,p2End);
		
		// UNDO PASS #3
		// Initialise start/end values
		int p3Start = p1End;
		int p3End = p1End;
		// Pass #3: UNDO from end of log until have undone all operations for transactions in undo list
		for(int i = p3End;i >= 0;i--) {
			LogRecord currRecord = records.get(i);
			if(undoList.contains(currRecord.toString())) {
				p3Start = i;
				if(currRecord.getItem() != null) {
					if(currRecord.getItem().equals("A")) {
						db.put("A",currRecord.getInitialValue());
					}
					if(currRecord.getItem().equals("B")) {
						db.put("B",currRecord.getInitialValue());
					}
					if(currRecord.getItem().equals("C")) {
						db.put("C",currRecord.getInitialValue());
					}
					if(currRecord.getItem().equals("D")) {
						db.put("D",currRecord.getInitialValue());
					}
					if(currRecord.getItem().equals("E")) {
						db.put("E",currRecord.getInitialValue());
					}
				}
			}
		}
		// Record start and end of pass #3
		db.setEndPass(3,p3Start);
		db.setStartPass(3,p3End);

		// Final statements
		System.out.println(db);
		return db;
	}

	/**
	 * Reads a log file where each record is comma-separated and on its own line.
	 * 
	 * @param fileName
	 *                 file name and path to read
	 * @return
	 *         ArrayList of LogRecords
	 */
	public ArrayList<LogRecord> readLog(String fileName) {
		ArrayList<LogRecord> records = new ArrayList<LogRecord>();

		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String rec;

			while ((rec = reader.readLine()) != null) {
				LogRecord record = LogRecord.parse(rec);
				records.add(record);
			}
		} catch (Exception e) {
			System.out.println("File exception: " + e);
			e.printStackTrace();
			System.exit(1);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					System.out.println(e);
				}
			}
		}

		System.out.println(records);
		return records;
	}
}