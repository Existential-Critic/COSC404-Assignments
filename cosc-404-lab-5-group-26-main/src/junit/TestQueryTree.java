package junit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import textdb.operators.Operator;
import textdb.query.QueryTree;
import textdb.relation.Attribute;
import textdb.relation.Relation;
import textdb.relation.Schema;
import textdb.relation.Tuple;
import textdb.util.FileManager;

/**
 * Tests query plans for midterm practice question
 */
public class TestQueryTree {
	// Change this if needed to indicate where the data and output directories are.
	public static String DATA_DIR = "bin/data/";			
	public static String OUTPUT_DIR = "bin/output/";
	
	private static Schema schema;

	
	/**
	 * Initializes a schema with all the data files.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@BeforeAll
	public static void init() throws Exception {
		schema = new Schema();
		
		Attribute []rAttr = new Attribute[3];
		rAttr[0] = new Attribute("r_regionkey",Attribute.TYPE_INT,0);
		rAttr[1] = new Attribute("r_name",Attribute.TYPE_STRING,15);
		rAttr[2] = new Attribute("r_comment",Attribute.TYPE_STRING,60);			
		Relation region = new Relation(rAttr, "region", DATA_DIR+"region.txt");
		schema.addTable(region);
		
		Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", DATA_DIR+"nation.txt");
		schema.addTable(nation);
		
		Attribute []attrs = new Attribute[8];
		attrs[0] = new Attribute("c_custkey",Attribute.TYPE_INT,0);
		attrs[1] = new Attribute("c_name",Attribute.TYPE_STRING,25);
		attrs[2] = new Attribute("c_address",Attribute.TYPE_STRING,75);
		attrs[3] = new Attribute("c_nationkey",Attribute.TYPE_INT,0);
		attrs[4] = new Attribute("c_phone",Attribute.TYPE_STRING,15);
		attrs[5] = new Attribute("c_acctbal",Attribute.TYPE_DOUBLE,0);
		attrs[6] = new Attribute("c_mktsegment",Attribute.TYPE_STRING,20);
		attrs[7] = new Attribute("c_comment",Attribute.TYPE_STRING,200);				
		Relation customer = new Relation(attrs, "customer", DATA_DIR+"customer.txt");
		schema.addTable(customer);

		// This code can be used to test if a basic scan works for a given input file
		//TextFileScan tblScan = new TextFileScan(DATA_DIR+"customer.txt", customer);
		//int count = compareOperatorWithOutput(tblScan, OUTPUT_DIR+"scanOutput.txt");	
	}

	/**
	 * Tests query1.	
	 */
	@Test
    public void testQuery1()
	{					           	
		try
		{
			Operator op = QueryTree.query1();
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"query1.txt");		
			assertEquals(4, count);
		}
		catch (Exception e)
		{	System.out.println(e);
			fail(); 
		}
	 }
	
	 /**
	 * Tests query2.	
	 */
	@Test
    public void testQuery2()
	{					           	
		try
		{
			Operator op = QueryTree.query2();			
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"query2.txt");		
			assertEquals(20, count);
		}
		catch (Exception e)
		{	System.out.println(e);
			fail(); 
		}
	}
	
	/**
	 * Compares the output of an operator with the expected output stored in a file.
	 * Returns a count of the number of records output by the operator.
	 * 
	 * @param op
	 * 		operator
	 * @param fileName
	 * 		name of file with data to compare
	 * @return
	 * 		number of records output by operator
	 */
	public static int compareOperatorWithOutput(Operator op, String fileName)
	{
		long startTime = System.currentTimeMillis();		
		String opOutput, fileOutput;	
		ArrayList<String> differences = new ArrayList<String>();
		
		if (op == null)
			return 0;			// Remember to initialize the operator
			
		try
		{
			BufferedReader reader = FileManager.openTextInputFile(fileName);
			op.init();
	
			// Tuple t = new Tuple(op.getOutputRelation());
			Tuple t = new Tuple();
			int count = 0;			
			
			while ( (t = op.next()) != null)
			{
				System.out.println(t);				// Should comment this out for large files
				// t.writeText(out);
				opOutput = t.toString().trim();
				if (reader.ready())
					fileOutput = reader.readLine().trim();
				else
					fileOutput = "";
				
				if (!opOutput.equals(fileOutput))
					differences.add("Yours: "+opOutput+" Solution: "+fileOutput);
				count++;
				if (count % 10000 == 0)
					System.out.println("Total results: "+op.getTuplesOutput()+" in time: "+(System.currentTimeMillis()-startTime));
			}
			FileManager.closeFile(reader);
			op.close();
		}
		catch (Exception e)
		{
			System.out.println("ERROR: "+e);
			fail();
		}		
		
		long endTime = System.currentTimeMillis();
		System.out.println("Total results: "+op.getTuplesOutput()+" in time: "+(endTime-startTime));
		
		if (differences.size() == 0)
			System.out.println("NO DIFFERENCES!");
		else
		{
			System.out.println("DIFFERENCES: "+differences.size());
			for (int i=0; i < differences.size(); i++)
				System.out.println(differences.get(i));
			fail();
		}
		return op.getTuplesOutput();
	}			
}
