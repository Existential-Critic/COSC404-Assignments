package textdb;

import java.io.*;

/**
 * A generic class for all operators (scan, projection, join).
 */
public abstract class Operator {
	protected Operator[] input; 			// Input operators
	protected int numInputs; 				// Number of inputs to this operator
	protected Relation outputRelation; 		// Output relation produced by this operator

	// I/O related properties
	protected int BUFFER_SIZE; 				// Number of pages that can be buffered by this operation
	protected int BLOCKING_FACTOR; 			// Number of tuples per page
	protected int tuplesOutput; 			// Total # of tuples output of this operator
	protected int tuplesRead; 				// Total # of tuples read by this operator from its inputs only
	protected int pagesRead; 				// Total # of pages read by this operator from its inputs only
	protected int internalTupleIOs; 		// Total # of tuples read/written by this operator for its own processing (not
											// including read from its sources)
	protected int internalPageIOs; 			// Total # of pages read/written by this operator for its own processing (not
											// including read from its sources)

	Operator() {
		this(null, 0, 0);
	}

	Operator(Operator[] in, int bfr, int bs) {
		if (in == null)
			numInputs = 0;
		else
			numInputs = in.length;
		input = in;
		BLOCKING_FACTOR = bfr;
		BUFFER_SIZE = bs;
		tuplesOutput = 0;
		tuplesRead = 0;
		pagesRead = 0;
		internalTupleIOs = 0;
		internalPageIOs = 0;
	}

	// Iterator methods
	abstract public void init() throws IOException;

	abstract public Tuple next() throws IOException;

	/*
	 * Note: Do NOT assume all operators implement hasNext().
	 */
	public boolean hasNext() throws IOException {
		return false;
	}

	public void close() throws IOException {
		for (int i = 0; i < numInputs; i++)
			input[i].close();
	}

	// By default, operators are not buffered/threaded
	public boolean isBuffered() {
		return false;
	}
	
	public void setOutputRelation(Relation r) {
		outputRelation = r;
	}

	public Relation getOutputRelation() {
		return outputRelation;
	}

	// Tuple and page counter methods
	protected void incrementTuplesOutput() {
		tuplesOutput++;
	}

	protected void incrementTuplesRead() {
		tuplesRead++;
	}

	protected void incrementPagesRead() {
		pagesRead++;
	}

	protected void incrementTupleIOs() {
		internalTupleIOs++;
	}

	protected void incrementPageIOs() {
		internalPageIOs++;
	}

	protected void incrementTuplesRead(int i) {
		tuplesRead += i;
	}

	protected void incrementPagesRead(int i) {
		pagesRead += i;
	}

	protected void incrementTuplesOutput(int i) {
		tuplesOutput += i;
	}

	protected void incrementTupleIOs(int i) {
		internalTupleIOs += i;
	}

	protected void incrementPageIOs(int i) {
		internalPageIOs += i;
	}

	public int getTuplesOutput() {
		return tuplesOutput;
	}

	public int getTuplesRead() {
		return tuplesRead;
	}

	public int getPagesRead() {
		return pagesRead;
	}

	public int getTupleIOs() {
		return internalTupleIOs;
	}

	public int getPageIOs() {
		return internalPageIOs;
	}
}
