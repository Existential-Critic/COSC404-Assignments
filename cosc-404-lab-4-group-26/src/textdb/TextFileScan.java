package textdb;

import java.io.*;

// Performs a file scan in iterator form.  File is assumed to be on disk in TEXT form.  
public class TextFileScan extends Operator {
	protected String inFileName;					// Name of input file to scan
	protected BufferedReader inFile;				// Used to read from text file
	protected Relation inputRelation;				// Schema of file being scanned


	public TextFileScan(String inName, Relation r) {
		super();
		inFileName = inName;
		inputRelation = r;
		setOutputRelation(r);						// Set output relation of this operator
	}

	public void init() throws FileNotFoundException, IOException {
		inFile = FileManager.openTextInputFile(inFileName);
	}

	public Tuple next() throws IOException {
		Tuple t = null;
		t = new Tuple(inputRelation);
		if(!t.readText(inFile)) {
			return null;
		}

		incrementTuplesRead();		
		incrementTuplesOutput();
		return t;
	}

	public boolean hasNext() throws IOException {
		return inFile.ready();
	}

	public void close() throws IOException {
		FileManager.closeFile(inFile);
	}
}

