package textdb.predicates;

import java.sql.SQLException;

import textdb.relation.Relation;
import textdb.relation.Tuple;

/**
 * A predicate implementing logical Or.
 */
public class Or extends SelectionPredicate
{	
	/**
	 * First input predicate
	 */
	private SelectionPredicate predicate1;
	
	/**
	 * Second input predicate
	 */
	private SelectionPredicate predicate2;

	/**
	 * Constructor for OR predicate.
	 * 
	 * @param p1
	 * 		first input predicate
	 * @param p2
	 * 		second input predicate
	 */
	public Or(SelectionPredicate p1, SelectionPredicate p2)
	{	
		this.predicate1 = p1;
		this.predicate2 = p2;
	}

	@Override
	public boolean evaluate(Tuple t) throws SQLException
	{	
		boolean b = this.predicate1.evaluate(t);
		if (b)
			return true;

		return this.predicate2.evaluate(t);
	}
	
	@Override
	public String toString(Relation relation)
	{	return this.predicate1.toString(relation)+" OR "+this.predicate2.toString(relation);
	}
}

