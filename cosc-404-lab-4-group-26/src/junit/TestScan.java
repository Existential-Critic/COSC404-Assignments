package junit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import java.io.BufferedReader;
import java.util.ArrayList;

import textdb.Attribute;
import textdb.FileManager;
import textdb.Operator;
import textdb.Relation;
import textdb.TextFileScan;
import textdb.Tuple;

/**
 * Tests the linear scan implementation.
 */
public class TestScan {	
    	
	public static String DATA_DIR = "bin/data/";			// Change this if needed to indicate where the data and output directories are.
	public static String OUTPUT_DIR = "bin/output/";
	
	private static Relation r;
	
	@BeforeAll
	public static void init() throws Exception {
		Attribute []attrs = new Attribute[5];

		attrs[0] = new Attribute("key",Attribute.TYPE_INT,0);
		attrs[1] = new Attribute("seq",Attribute.TYPE_INT,0);
		attrs[2] = new Attribute("v1",Attribute.TYPE_INT,0);
		attrs[3] = new Attribute("v2",Attribute.TYPE_INT,0);
		attrs[4] = new Attribute("text",Attribute.TYPE_STRING,100);

		r = new Relation(attrs);
	}	
   
	@Test
    public void testSmallScan()
	{   System.out.println("\n\nTesting text file scan."); 					
		TextFileScan tblScan = new TextFileScan(DATA_DIR+"smallInputLeft.txt", r);
		int count = compareOperatorWithOutput(tblScan, OUTPUT_DIR+"scanOutput.txt");		
		assertEquals(101, count);
	}
   
	/**
	 * Compares the output of an operator with the expected output stored in a file.
	 * Returns a count of the number of records output by the operator.
	 * @param op
	 * @param reader
	 * @return
	 */
	public static int compareOperatorWithOutput(Operator op, String fileName)
	{
		long startTime = System.currentTimeMillis();		
		String opOutput, fileOutput;	
		ArrayList<String> differences = new ArrayList<String>();
		
		try
		{
			BufferedReader reader = FileManager.openTextInputFile(fileName);
			op.init();
	
			Tuple t = new Tuple(op.getOutputRelation());
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
			e.printStackTrace();
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
