package junit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import textdb.Attribute;
import textdb.EquiJoinPredicate;
import textdb.NestedLoopJoin;
import textdb.Operator;
import textdb.Relation;
import textdb.TextFileScan;

/**
 * Tests nested loop join implementation.
 */
public class TestNestedLoopJoin {

	// Change this if needed to indicate where the data and output directories are.
	public static String DATA_DIR = "bin/data/";
	public static String OUTPUT_DIR = "bin/output/";

	private static Relation r;

	@BeforeAll
	public static void init() throws Exception {
		Attribute[] attrs = new Attribute[5];

		attrs[0] = new Attribute("key", Attribute.TYPE_INT, 0);
		attrs[1] = new Attribute("seq", Attribute.TYPE_INT, 0);
		attrs[2] = new Attribute("v1", Attribute.TYPE_INT, 0);
		attrs[3] = new Attribute("v2", Attribute.TYPE_INT, 0);
		attrs[4] = new Attribute("text", Attribute.TYPE_STRING, 100);

		r = new Relation(attrs);
	}

	@Test
	public void testMediumJoin() {
		System.out.println("\n\nTesting medium nested loop join.");
		TextFileScan scanLeft = new TextFileScan(DATA_DIR + "mediumInputLeft.txt", r);
		TextFileScan scanRight = new TextFileScan(DATA_DIR + "mediumInputRight.txt", r);
		EquiJoinPredicate ep = new EquiJoinPredicate(new int[] { 0 }, new int[] { 0 }, EquiJoinPredicate.INT_KEY);

		NestedLoopJoin nloop = new NestedLoopJoin(new Operator[] { scanLeft, scanRight }, ep);
		int count = TestScan.compareOperatorWithOutput(nloop, OUTPUT_DIR + "nestedOutputMedium.txt");
		assertEquals(127, count);
	}

	@Test
	public void testLargeJoin() {
		System.out.println("\n\nTesting large nested loop join.");
		TextFileScan scanLeft = new TextFileScan(DATA_DIR + "largeInputLeft.txt", r);
		TextFileScan scanRight = new TextFileScan(DATA_DIR + "largeInputRight.txt", r);
		EquiJoinPredicate ep = new EquiJoinPredicate(new int[] { 0 }, new int[] { 0 }, EquiJoinPredicate.INT_KEY);

		NestedLoopJoin nloop = new NestedLoopJoin(new Operator[] { scanLeft, scanRight }, ep);

		int count = TestScan.compareOperatorWithOutput(nloop, OUTPUT_DIR + "nestedOutputLarge.txt");
		assertEquals(191, count);
	}
}
