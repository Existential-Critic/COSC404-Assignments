package textdb.operators;

import java.io.IOException;

import textdb.functions.Expression;
import textdb.relation.Relation;
import textdb.relation.Tuple;


/**
 * Performs relational projection (including attribute renaming and reordering).
 */
public class Projection extends Operator
{		
	/**
	 * Expressions evaluated in projection (may just be simple attributes)
	 */
	protected Expression[] expressionList;			
	
	/**
	 * Input operator
	 */
	private Operator input;
	
	public Projection(Operator in, Expression[] exprList, Relation outputRelation)
	{	super(new Operator[] {in}, 0, 0);
		input = in;
		expressionList = exprList;
		setOutputRelation(outputRelation);
	}

	public void init() throws IOException
	{	input.init();
	}

	public Tuple next() throws IOException
	{	Tuple inTuple = input.next();
		incrementTuplesRead();

		if (inTuple == null)
			return null;		
		
		Object[] vals = new Object[this.expressionList.length];
		for (int i=0; i < this.expressionList.length; i++)
			if(expressionList[i] != null) 
			{	
				vals[i] = expressionList[i].evaluate(inTuple);
			}
		
		incrementTuplesOutput();
		return new Tuple(vals,getOutputRelation());
	}

	public void close() throws IOException
	{	super.close();
	}
	
	public String toString()
	{	StringBuffer sb = new StringBuffer(250);
		sb.append("PROJECT: ");
		sb.append(expressionList[0].toString(input.getOutputRelation()));
		for (int i=1; i < expressionList.length; i++)
			sb.append(", "+expressionList[i].toString(input.getOutputRelation()));
		return sb.toString();
	}
}

