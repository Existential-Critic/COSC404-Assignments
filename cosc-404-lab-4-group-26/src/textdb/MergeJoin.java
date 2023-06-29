package textdb;

import java.io.*;

/**
 * Contains code for performing an external merge join in iterator format.
 */
public class MergeJoin extends Operator {
	private int MERGE_BUFFER_SIZE = 10000; // The number of tuples that can be buffered with the same key.
	private EquiJoinPredicate pred; // A equi-join comparison class that can handle 1 or more attributes

	// Iterator state variables
	private Tuple tupleLeft;
	private Tuple tupleRight;

	public MergeJoin(Operator[] in, EquiJoinPredicate p) {
		super(in, 0, 0);
		pred = p;
	}

	public void init() throws IOException {
		input[0].init();
		input[1].init();

		// Create output relation - keep all attributes of both tuples
		Relation out = new Relation(input[0].getOutputRelation());
		out.mergeRelation(input[1].getOutputRelation());
		setOutputRelation(out);

		// TODO: YOUR SETUP CODE HERE
		tupleLeft = input[0].next();
	}

	public Tuple next() throws IOException {
		/*do {
			tupleRight = input[1].next();
			if(tupleRight == null) {
				input[1].close();
				tupleLeft = input[0].next();
				if(tupleLeft == null) {
					return null;
				}
				input[1].init();		// Reinitialise scan of input[0]
				tupleRight = input[1].next();
			}
		} while(!pred.isEqual(tupleLeft,tupleRight));
		return outputJoinTuple(tupleLeft, tupleRight);
		*/
		while (tupleLeft != null) {
			tupleRight = input[1].next();
			while (tupleRight != null) {
				if(pred.isEqual(tupleLeft,tupleRight)) {
					outputJoinTuple(tupleLeft, tupleRight);
				}
				tupleRight = input[1].next();
			}
			input[1].init();
			tupleLeft = input[0].next();
		}
		return null;
	}

	public void close() throws IOException {
		super.close();
	}

	private Tuple outputJoinTuple(Tuple left, Tuple right) {
		Tuple t = new Tuple(left, right, getOutputRelation());
		incrementTuplesOutput();
		return t;
	}
}
