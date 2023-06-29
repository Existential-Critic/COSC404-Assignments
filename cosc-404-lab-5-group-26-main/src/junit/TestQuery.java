package junit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import textdb.operators.Operator;
import textdb.parser.SimpleNode;
import textdb.query.Optimizer;
import textdb.relation.Attribute;
import textdb.relation.Relation;
import textdb.relation.Schema;
import textdb.relation.Tuple;
import textdb.util.FileManager;

/**
 * Tests ORDER BY query parsing and execution.
 */
public class TestQuery {
	
	public static String DATA_DIR = "bin/data/";			// Change this if needed to indicate where the data and output directories are.
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
		
		Attribute []ordAttr = new Attribute[9];
		ordAttr[0] = new Attribute("o_orderkey",Attribute.TYPE_INT,4);
		ordAttr[1] = new Attribute("o_custkey",Attribute.TYPE_INT,4);
		ordAttr[2] = new Attribute("o_orderstatus",Attribute.TYPE_STRING,1);
		ordAttr[3] = new Attribute("o_totalprice",Attribute.TYPE_STRING,9);
		ordAttr[4] = new Attribute("o_orderdate",Attribute.TYPE_STRING,10);	// use 10 instead of 8
		ordAttr[5] = new Attribute("o_orderpriority",Attribute.TYPE_STRING,15);
		ordAttr[6] = new Attribute("o_clerk",Attribute.TYPE_STRING,15);
		ordAttr[7] = new Attribute("o_shippriority",Attribute.TYPE_INT,4);
		ordAttr[8] = new Attribute("o_comment",Attribute.TYPE_STRING,79);
		Relation orders = new Relation(ordAttr, "orders", DATA_DIR+"orders.txt");
		schema.addTable(orders);
		
		Attribute []partAttr = new Attribute[9];
		partAttr[0] = new Attribute("p_partkey",Attribute.TYPE_INT,0);
		partAttr[1] = new Attribute("p_name",Attribute.TYPE_STRING,55);
		partAttr[2] = new Attribute("p_mfgr",Attribute.TYPE_STRING,25);
		partAttr[3] = new Attribute("p_brand",Attribute.TYPE_STRING,10);
		partAttr[4] = new Attribute("p_type",Attribute.TYPE_STRING,25);
		partAttr[5] = new Attribute("p_size",Attribute.TYPE_INT,0);
		partAttr[6] = new Attribute("p_container",Attribute.TYPE_STRING,10);
		partAttr[7] = new Attribute("p_retailprice",Attribute.TYPE_STRING,9);
		partAttr[8] = new Attribute("p_comment",Attribute.TYPE_STRING,23);
		Relation part = new Relation(partAttr, "part", DATA_DIR+"part.txt");
		schema.addTable(part);
		
		Attribute []suppAttr = new Attribute[7];
		suppAttr[0] = new Attribute("s_suppkey",Attribute.TYPE_INT,0);
		suppAttr[1] = new Attribute("s_name",Attribute.TYPE_STRING,25);
		suppAttr[2] = new Attribute("s_address",Attribute.TYPE_STRING,40);
		suppAttr[3] = new Attribute("s_nationkey",Attribute.TYPE_INT,0);
		suppAttr[4] = new Attribute("s_phone",Attribute.TYPE_STRING,15);
		suppAttr[5] = new Attribute("s_acctbal",Attribute.TYPE_STRING,9);
		suppAttr[6] = new Attribute("s_comment",Attribute.TYPE_STRING,101);
		Relation supplier = new Relation(suppAttr, "supplier", DATA_DIR+"supplier.txt");
		schema.addTable(supplier);
		
		Attribute []psAttr = new Attribute[5];
		psAttr[0] = new Attribute("ps_partkey",Attribute.TYPE_INT,0);
		psAttr[1] = new Attribute("ps_suppkey",Attribute.TYPE_INT,0);
		psAttr[2] = new Attribute("ps_availqty",Attribute.TYPE_INT,0);
		psAttr[3] = new Attribute("ps_supplycost",Attribute.TYPE_STRING,9);
		psAttr[4] = new Attribute("ps_comment",Attribute.TYPE_STRING,199);
		Relation partsupp = new Relation(psAttr, "partsupp", DATA_DIR+"partsupp.txt");
		schema.addTable(partsupp);
		
		Attribute []liAttr = new Attribute[16];
		liAttr[0] = new Attribute("l_orderkey",Attribute.TYPE_INT,4);
		liAttr[1] = new Attribute("l_partkey",Attribute.TYPE_INT,4);
		liAttr[2] = new Attribute("l_suppkey",Attribute.TYPE_INT,4);
		liAttr[3] = new Attribute("l_linenumber",Attribute.TYPE_INT,4);
		liAttr[4] = new Attribute("l_quantity",Attribute.TYPE_STRING,9);
		liAttr[5] = new Attribute("l_extendedprice",Attribute.TYPE_STRING,9);
		liAttr[6] = new Attribute("l_discount",Attribute.TYPE_STRING,9);
		liAttr[7] = new Attribute("l_tax",Attribute.TYPE_STRING,9);
		liAttr[8] = new Attribute("l_returnflag",Attribute.TYPE_STRING,1);
		liAttr[9] = new Attribute("l_linestatus",Attribute.TYPE_STRING,1);
		liAttr[10] = new Attribute("l_shipdate",Attribute.TYPE_STRING,10);	// made all dates size 10 instead of 8 as that is their strength length
		liAttr[11] = new Attribute("l_commitdate",Attribute.TYPE_STRING,10);// here to
		liAttr[12] = new Attribute("l_receiptdate",Attribute.TYPE_STRING,10); //here to
		liAttr[13] = new Attribute("l_shipinstruct",Attribute.TYPE_STRING,25);
		liAttr[14] = new Attribute("l_shipmode",Attribute.TYPE_STRING,10);
		liAttr[15] = new Attribute("l_comment",Attribute.TYPE_STRING,44);
		Relation lineitem = new Relation(liAttr, "lineitem", DATA_DIR+"lineitem.txt");
		schema.addTable(lineitem);
		
		// This code can be used to test if a basic scan works for a given input file
		//TextFileScan tblScan = new TextFileScan(DATA_DIR+"customer.txt", customer);
		//int count = compareOperatorWithOutput(tblScan, OUTPUT_DIR+"scanOutput.txt");	
	}

	/**
	 * Tests ORDER BY attr DESC.	
	 */
	@Test
    public void testParseOneTableOrderByDesc(){					          
		Optimizer opt = new Optimizer();
      	
		try
		{
			SimpleNode root = opt.buildParseTree("SELECT n_name FROM nation ORDER BY n_name DESC");
			String answer = Optimizer.outputPlan(root);
			System.out.println(answer);
			String result = "Start"
							+"\n   Select"
							+"\n      Identifier: n_name"							
							+"\n   From"
							+"\n      Identifier: nation"
							+"\n   Orderby"
							+"\n      Identifier: n_name"
							+"\n      DESC\n";
			assertEquals(result, answer);
		}
		catch (SQLException e)
		{	System.out.println(e);
			fail(); 
		}
	 }
	
	/**
	 * Tests ORDER BY attr ASC.	
	 */
	@Test
    public void testParseOneTableOrderByAsc(){				          
		Optimizer opt = new Optimizer();
      	
		try
		{			
			SimpleNode root = opt.buildParseTree("SELECT n_name FROM nation ORDER BY n_name ASC");
			String answer = Optimizer.outputPlan(root);
			System.out.println(answer);
			String result = "Start"
							+"\n   Select"
							+"\n      Identifier: n_name"							
							+"\n   From"
							+"\n      Identifier: nation"
							+"\n   Orderby"
							+"\n      Identifier: n_name"
							+"\n      ASC\n";
			assertEquals(answer, result);
		}
		catch (SQLException e)
		{	System.out.println(e);
			fail(); 
		}
	 }
	
	/**
	 * Tests ORDER BY attr.	
	 */
	@Test
    public void testParseOneTableOrderByNone(){				           
		Optimizer opt = new Optimizer();
      	
		try
		{			
			SimpleNode root = opt.buildParseTree("SELECT n_name FROM nation ORDER BY n_name");
			String answer = Optimizer.outputPlan(root);
			System.out.println(answer);
			String result = "Start"
							+"\n   Select"
							+"\n      Identifier: n_name"							
							+"\n   From"
							+"\n      Identifier: nation"
							+"\n   Orderby"
							+"\n      Identifier: n_name\n";								
			assertEquals(answer, result);
		}
		catch (SQLException e)
		{	System.out.println(e);
			fail(); 
		}
	 }
	
	/**
	 * Tests ORDER BY attr1 ASC, attr2 DESC.	
	 */
	@Test
    public void testParseOneTableMultipleOrderByNone(){				          
		Optimizer opt = new Optimizer();
      	
		try
		{
			SimpleNode root = opt.buildParseTree("SELECT n_name, n_nationkey FROM nation ORDER BY n_name ASC, n_nationkey DESC");
			String answer = Optimizer.outputPlan(root);
			System.out.println(answer);
			String result = "Start"
							+"\n   Select"
							+"\n      Identifier: n_name"
							+"\n      Identifier: n_nationkey"
							+"\n   From"
							+"\n      Identifier: nation"
							+"\n   Orderby"
							+"\n      Identifier: n_name"
							+"\n      ASC"
							+"\n      Identifier: n_nationkey"
							+"\n      DESC\n";
			assertEquals(answer, result);
		}
		catch (SQLException e)
		{	System.out.println(e);
			fail(); 
		}
	 }
	
	/**
	 * Tests execution of ORDER BY attr1 DESC.	
	 */
	@Test
    public void testExecOneTableOrderByDesc(){					           
		Optimizer opt = new Optimizer();
      	
		try
		{
			Operator op = opt.buildPlan(schema, "SELECT n_name FROM nation ORDER BY n_name DESC");				
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"sortDescOutput.txt");		
			assertEquals(count, 25);
		}
		catch (SQLException e)
		{	System.out.println(e);
			fail(); 
		}
	 }
	
	/**
	 * Tests execution of ORDER BY attr1 ASC.	
	 */
	@Test
    public void testExecOneTableOrderByAsc(){				           
		Optimizer opt = new Optimizer();
      	
		try
		{
			Operator op = opt.buildPlan(schema, "SELECT n_name FROM nation ORDER BY n_name ASC");			
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"sortAscOutput.txt");		
			assertEquals(count, 25);
					
		}
		catch (SQLException e)
		{	System.out.println(e);
			fail(); 
		}
	 }

	/**
	 * Tests execution of ORDER BY attr1.	
	 */
	@Test
    public void testExecOrderByJoinNoAsc(){					           
		Optimizer opt = new Optimizer();
      	
		try
		{
			Operator op = opt.buildPlan(schema, "SELECT n_nationkey, n_name, r_name FROM nation INNER JOIN region on n_regionkey = r_regionkey WHERE r_regionkey < 3 ORDER BY n_name");
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"sortJoinOutput.txt");		
			assertEquals(count, 15);
		}
		catch (SQLException e)
		{	System.out.println(e);
			fail(); 
		}
	 }
	
	/**
	 * Outputs an operator result to standard output.
	 * 
	 * @param op
	 * 		operator
	 * @return
	 * 		number of rows output
	 */
	public static int outputOperator(Operator op)
	{
		try
		{			
			op.init();
	
			Tuple t = new Tuple(op.getOutputRelation());
			int count = 0;			
			
			while ( (t = op.next()) != null)
			{
				System.out.println(t);				// Should comment this out for large files			
				count++;
		//		if (count % 10000 == 0)
		//			System.out.println("Total results: "+op.getTuplesOutput()+" in time: "+(System.currentTimeMillis()-startTime));
			}
			System.out.println("Total results: "+count);
			op.close();
			return count;
		}
		catch (Exception e)
		{
			System.out.println("ERROR: "+e);
			fail();
		}	
		return 0;
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
