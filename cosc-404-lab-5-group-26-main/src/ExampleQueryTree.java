import java.io.FileNotFoundException;
import java.io.IOException;

import textdb.functions.Expression;
import textdb.functions.ExtractAttribute;
import textdb.operators.MergeSort;
import textdb.operators.Operator;
import textdb.operators.Projection;
import textdb.operators.TextFileScan;
import textdb.predicates.SortComparator;
import textdb.relation.Attribute;
import textdb.relation.Relation;
import textdb.relation.Tuple;

public class ExampleQueryTree {
    public static void main(String[]argv) throws FileNotFoundException, IOException
    {
        Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", "bin/data/nation.txt");

        TextFileScan scan = new TextFileScan("bin/data/nation.txt", nation);

        Operator op;
        // Q1: What would you change to sort by n_name ?
        // Hint: Look at MergeSort and SortComparator        

        // Q2: Add projection on n_nationkey, n_name
        // Hint: Projection operator and ExtractAttribute for expressions             
        
        op = scan;
      
        op.init();

        Tuple t;
        t = op.next();
        while (t != null)
        {	System.out.println(t);
            t = op.next();
        }
        op.close();
    }
}
