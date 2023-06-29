package textdb.query;

import java.io.FileNotFoundException;
import java.io.IOException;

import textdb.functions.Expression;
import textdb.functions.ExtractAttribute;
import textdb.operators.BlockNestedLoopJoin;
import textdb.operators.MergeSort;
import textdb.operators.Operator;
import textdb.operators.Projection;
import textdb.operators.Selection;
import textdb.operators.TextFileScan;
import textdb.predicates.SelectionPredicate;
import textdb.predicates.ConstantSelectionPredicate;
import textdb.predicates.Equal;
import textdb.predicates.EquiJoinPredicate;
import textdb.predicates.Greater;
import textdb.predicates.Less;
import textdb.predicates.Or;
import textdb.predicates.SortComparator;
import textdb.relation.Attribute;
import textdb.relation.Relation;
import textdb.relation.Tuple;

public class QueryTree {

    // Change this if needed to indicate where the data and output directories are.
	public static String DATA_DIR = "bin/data/";			
	public static String OUTPUT_DIR = "bin/output/";

    public static void main(String[]argv) throws FileNotFoundException, IOException {
        example1();
        example();

        Operator op = query1();
        outputOperator(op);
        op = query2();
        outputOperator(op);
    }


    // This code builds a query tree for the SQL: SELECT n_nationkey, n_name FROM nation ORDER BY n_name ASC
    public static void example1() throws FileNotFoundException, IOException {
        System.out.println("\nExample Query Plan: SELECT n_nationkey, n_name FROM nation ORDER BY n_name ASC");

        Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", DATA_DIR+"nation.txt");

        // Table scan operator
        TextFileScan nscan = new TextFileScan(DATA_DIR+"nation.txt", nation);

        // Sort operator
        // n_name attribute is at index 1 of nation
        SortComparator sc = new SortComparator(new int[]{1}, new boolean[]{true});
        MergeSort sorter = new MergeSort(nscan, 1000, 10, sc);

        // Projection operator              
        Expression[] expr = new Expression[2];
        expr[0] = new ExtractAttribute(0);  
        expr[1] = new ExtractAttribute(1);          
        Projection proj = new Projection(sorter, expr, sorter.getOutputRelation());
        
        Operator op;        
        op = proj;
      
        outputOperator(op);        
    }

    public static void outputOperator(Operator op) throws IOException {
        if (op == null)
            return;   
        op.init();
        Tuple t;
        t = op.next();
        while (t != null) {
            System.out.println(t);
            t = op.next();
        }
        op.close();
    }


    // This code builds a query tree for the SQL: SELECT c_name, n_nationkey, n_name FROM nation JOIN customer ON n_nationkey=c_nationkey WHERE n_nationkey = 1 and c_custkey < 50 ORDER BY c_name ASC  
    public static void example() throws FileNotFoundException, IOException {
        System.out.println("\nExample Query Plan: SELECT c_name, n_nationkey, n_name FROM nation JOIN customer ON n_nationkey=c_nationkey WHERE n_nationkey = 1 and c_custkey < 50 ORDER BY c_name ASC");

        Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", DATA_DIR+"nation.txt");

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

        // Table scan operator
        TextFileScan nscan = new TextFileScan(DATA_DIR+"nation.txt", nation);
        TextFileScan cscan = new TextFileScan(DATA_DIR+"customer.txt", customer);

        // Selection operator - n_nationkey = 1
        // First parameter is attribute index: n_nationkey (0)
        SelectionPredicate pred = new ConstantSelectionPredicate(0, 1, new Equal());
        Selection nsel = new Selection(nscan, pred);

        // Selection operator - c_custkey < 50
        // First parameter is attribute index: c_custkey (0)
        SelectionPredicate pred2 = new ConstantSelectionPredicate(0, 50, new Less());
        Selection csel = new Selection(cscan, pred2);

        // Join operator
        // Indexes for join predicate are attribute index in each relation: n_nationkey (0), c_nationkey (3)
        EquiJoinPredicate jpred = new EquiJoinPredicate(new int[]{0}, new int[]{3}, 1);
        BlockNestedLoopJoin nlj = new BlockNestedLoopJoin(new Operator[]{nsel, csel}, jpred, 1000, 10);

        // Sort operator
        // After join output relation is 4 attributes of nation and then attributes of customer.
        // c_name attribute is at index 5 of join result
        SortComparator sc = new SortComparator(new int[]{5}, new boolean[]{true});
        MergeSort sorter = new MergeSort(nlj, 1000, 10, sc);

        // Projection operator
        Expression[] expr = new Expression[3];
        expr[0] = new ExtractAttribute(5);  
        expr[1] = new ExtractAttribute(0);  
        expr[2] = new ExtractAttribute(1);        
        Projection proj = new Projection(sorter, expr, sorter.getOutputRelation());
        
        Operator op;        
        op = proj;
      
        outputOperator(op);
    }

    // TODO: Build a query tree for the SQL: SELECT s_suppkey, s_name, s_phone FROM supplier WHERE s_suppkey < 5 ORDER BY s_phone ASC 
    public static Operator query1() throws FileNotFoundException, IOException {
        System.out.println("\n\nQ1. Query Plan for: SELECT s_suppkey, s_name, s_phone FROM supplier WHERE s_suppkey < 5 ORDER BY s_phone ASC");

        Attribute []suppAttr = new Attribute[7];
		suppAttr[0] = new Attribute("s_suppkey",Attribute.TYPE_INT,0);
		suppAttr[1] = new Attribute("s_name",Attribute.TYPE_STRING,25);
		suppAttr[2] = new Attribute("s_address",Attribute.TYPE_STRING,40);
		suppAttr[3] = new Attribute("s_nationkey",Attribute.TYPE_INT,0);
		suppAttr[4] = new Attribute("s_phone",Attribute.TYPE_STRING,15);
		suppAttr[5] = new Attribute("s_acctbal",Attribute.TYPE_STRING,9);
		suppAttr[6] = new Attribute("s_comment",Attribute.TYPE_STRING,101);
		Relation supplier = new Relation(suppAttr, "supplier", DATA_DIR+"supplier.txt");

        // Table scan operator
        TextFileScan sscan = new TextFileScan(DATA_DIR+"supplier.txt",supplier);
       
        // Selection operator - s_suppkey < 5
        // Attribute index of s_suppkey is 0
        SelectionPredicate pred = new ConstantSelectionPredicate(0,5,new Less());
        Selection ssel = new Selection(sscan,pred);

        // Sort operator
        // s_phone attribute is at index 4 of supplier
        SortComparator sc = new SortComparator(new int[]{4},new boolean[]{true});
        MergeSort sorter = new MergeSort(ssel,1000,10,sc);

        // Projection operator
        Expression[] expr = new Expression[3];
        expr[0] = new ExtractAttribute(0);
        expr[1] = new ExtractAttribute(1);
        expr[2] = new ExtractAttribute(4); 
        Projection proj = new Projection(sorter,expr,sorter.getOutputRelation());

        Operator op;
        op = proj;

        // Note: Do not call outputOperator(op) like in example. That is done in test.
        return op;
    }

    /**
      TODO: Build a query tree for the SQL: 
                SELECT n_name, n_regionkey, n_nationkey, s_nationkey, s_name
                FROM nation JOIN supplier ON n_nationkey=s_nationkey
                WHERE (n_regionkey = 1 or n_regionkey = 3) AND s_suppkey < 50
                ORDER BY n_name ASC, s_name ASC
     */    
    public static Operator query2() throws FileNotFoundException, IOException {
        System.out.println("\n\nQ2. Query Plan for:  SELECT n_name, n_regionkey, n_nationkey, s_nationkey, s_name "
                                +" FROM nation JOIN supplier ON n_nationkey=s_nationkey "
                                +"WHERE (n_regionkey = 1 or n_regionkey = 3) AND s_suppkey < 50 "
                                +"ORDER BY n_name ASC, s_name ASC");

        Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", DATA_DIR+"nation.txt");

        Attribute []suppAttr = new Attribute[7];
		suppAttr[0] = new Attribute("s_suppkey",Attribute.TYPE_INT,0);
		suppAttr[1] = new Attribute("s_name",Attribute.TYPE_STRING,25);
		suppAttr[2] = new Attribute("s_address",Attribute.TYPE_STRING,40);
		suppAttr[3] = new Attribute("s_nationkey",Attribute.TYPE_INT,0);
		suppAttr[4] = new Attribute("s_phone",Attribute.TYPE_STRING,15);
		suppAttr[5] = new Attribute("s_acctbal",Attribute.TYPE_STRING,9);
		suppAttr[6] = new Attribute("s_comment",Attribute.TYPE_STRING,101);
		Relation supplier = new Relation(suppAttr, "supplier", DATA_DIR+"supplier.txt");

        // TODO: Create query plan.
        
        // TODO: Scan operators
        // scaning table nation
        TextFileScan nscan = new TextFileScan(DATA_DIR+"nation.txt",nation);
        TextFileScan sscan = new TextFileScan(DATA_DIR+"supplier.txt",supplier);

        // TODO: Selection operators
        // (n_regionkey = 1 or n_regionkey = 3)
        SelectionPredicate pred11 = new ConstantSelectionPredicate(2, 1, new Equal());
        SelectionPredicate pred12 = new ConstantSelectionPredicate(2, 3, new Equal());
        Selection nsel = new Selection(nscan, new Or(pred11, pred12));
        
        //s_suppkey < 50
        SelectionPredicate pred2 = new ConstantSelectionPredicate(0, 50, new Less());
        Selection ssel = new Selection(sscan, pred2);
        System.out.println(ssel);

        // TODO: Join operator
        EquiJoinPredicate pJoinPredicate = new EquiJoinPredicate(new int[]{0}, new int[]{3}, 1);
        BlockNestedLoopJoin blockNestedLoopJoin = new BlockNestedLoopJoin(new Operator[]{nsel, ssel}, pJoinPredicate, 1000, 10);
   
        // TODO: Projection operator
            
        // TODO: Sort operator
        SortComparator sc = new SortComparator(new int[]{1, 5}, new boolean[]{true, true});
        MergeSort sorter = new MergeSort(blockNestedLoopJoin, 1000, 10, sc);

        Expression[] expr = new Expression[5];
        expr[0] = new ExtractAttribute(1);
        expr[1] = new ExtractAttribute(2);
        expr[2] = new ExtractAttribute(0); 
        expr[3] = new ExtractAttribute(7);
        expr[4] = new ExtractAttribute(5);
        Projection proj = new Projection(sorter,expr,sorter.getOutputRelation());

        Operator op = proj;        

        // TODO: Assign your top query plan operator to op.
        // op = todo;

        // Note: Do not call outputOperator(op) like in example. That is done in test.
        return op;
    }
}
