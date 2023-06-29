package textdb;

/*
FILE: 			Tuple.java

Constructors
------------
public Tuple(Relation r)
 - creates empty tuple that has given relational schema

public Tuple(Object[] vals, Relation r)
 - initializes tuple values with array of Object references
 - does a very shallow copy!! i.e. does not copy array values (references) into a new array - just create reference to object array

public Tuple(Tuple t)
 - create new tuple using values from another tuple
 - performs a shallow copy:  Creates a new Object[] but does not create new objects themselves.
 - performs a shallow copy (does not actually construct new object for values but rather just has new tuple refer to objects of this tuple);
 - It probably should do a deep copy instead:
 - performs a deep copy (copies all objects to create new objects) i.e. just does not copy object references

public Tuple(Tuple t1, Tuple t2, Relation r)
 - creates a new tuple by appending the values of two tuples together (**Performs shallow copy.**)
 - r is the schema of the resulting relation


Get/Set Methods
---------------
public Object[] getValues()
public void setValues (Object[] vals)
public int numValues()
  - retrieve/set all tuple values

public String getString(int i)
public Object getObject(int i)
public int getInt(int i)
 - retrieve tuple attribute value i (indexed from 0) as a given type
 - assumes i is in range from 0 .. number of values - 1
 - note that may result in class cast errors


I/O Methods
-----------
public boolean readText(BufferedReader in) throws IOException
public void writeText(PrintWriter out)
 - read/write tuples in text format
 - if no schema is given, readText will create a new schema where all attributes are strings

public boolean read(BufferedReader in) throws IOException
public void write(PrintWriter out)
 - read/write tuples in binary format

Other Methods
-------------
public Object[] filterValues(int []idx)
 - idx[] contains the indexes of attributes to be put in the final result
 - This operation performs both projection and attribute reordering and returns a new array of attribute values.

public String toString()


Implementation Details
----------------------
- a tuple is an array of Object references in memory
- On disk tuple is of the form:
recordHeader numFields isNull[] offset[] fld1Data ... fldNData

 - recordHeader is a single byte that is not currently used
 - numFields is the number of fields in the record
 - isNull[] is an array of bits where a bit is 1 if the field is NULL.  A NULL field is not in the data portion.
 - offset[] is an array of field offsets (stored currently as 2 bytes each)
 - the data follows int the record
*/


import java.io.*;
import java.util.StringTokenizer;

/**
 * A generic structure for storing tuples.
 */
public class Tuple
{
	/**
	 * Array of field values
	 */
	protected Object[] values;						
	
	/**
	 * Relational schema describing tuple format
	 */
	protected Relation relation;						

	
	/**
	 * Copy constructor.
	 * @param r
	 * 		relation to copy
	 */
	public Tuple(Relation r) 						
	{	relation = r; 
		values = new Object[r.getNumAttributes()]; 
	}
	
	/**
	 * Initializes tuple values with array of Object references. Does a shallow copy of value references (but does not clone values).
	 * 
	 * @param vals
	 * 		array of values
	 * @param r
	 * 		relation describing tuple
	 */
	public Tuple(Object[] vals, Relation r) 		{
		relation = r;
		values = new Object[vals.length];
		for (int i=0; i < vals.length; i++)
			values[i] = vals[i];
	}

	/**
	 * Initializes tuple values with another tuple. Does a shallow copy of value references from other tuple (but does not clone values).
	 * 
	 * @param t
	 * 		tuple copied	 
	 */
	public Tuple(Tuple t)
	{	relation = t.relation;
		values = new Object[t.values.length];
		for (int i=0; i < t.values.length; i++)
			values[i] = t.values[i];
	}
	
	/**
	 * Creates a new tuple that consists of a relation and values from tuple 1 and tuple 2.
	 * 
	 * @param t1
	 * 		first tuple
	 * @param t2
	 * 		second tuple
	 * @param r
	 * 		relation describing new tuple
	 */
	public Tuple(Tuple t1, Tuple t2, Relation r)
	{	relation = r;
		values = new Object[t1.values.length+t2.values.length];
		for (int i=0; i < t1.values.length; i++)
			values[i] = t1.values[i];

		for (int i=0; i < t2.values.length; i++)
			values[i+t1.values.length] = t2.values[i];
	}
	
	/*
	 *  Utility Methods
	 */
	
	/**
	 * Produces a new tuple by only keeping attribute locations (indexed from 0) given in the input array.
	 * 
	 * @param idx
	 * 		attribute indexes to keep
	 * @return
	 * 		new array of values after filtering out values not wanted
	 */
	public Object[] filterValues(int []idx)
	{	Object[] tmp = new Object[idx.length];

		for (int i=0; i < idx.length; i++)
			tmp[i] = values[idx[i]];

		return tmp;
	}

	/**
	 * Adds a field value to the end of the tuple.
	 * 
	 * @param val
	 * 		value to add
	 */
	public void addField(Object val)
	{	Object[] tmp = new Object[values.length+1];
		for (int i=0; i < values.length; i++)
			tmp[i] = values[i];
		tmp[values.length] = val;
		values = tmp;
	}

	/**
	 * Retrieves the tuple values.
	 * 
	 * @return
	 * 		array of tuple values
	 */
	public Object[] getValues() 					
	{	return values; 
	}
	
	/**
	 * Sets the tuple values.
	 * 
	 * @param vals
	 * 		array of tuple values
	 */
	public void setValues (Object[] vals) 			
	{	values = vals; }
	
	/**
	 * Returns number of values in tuple.
	 * 
	 * @return
	 * 		number of values in tuple
	 */
	public int numValues()							
	{ 	return values.length; 
	}
	
	/**
	 * Sets a value at a given index (starting from 0).
	 * 
	 * @param i
	 * 		index of value (starting at 0)
	 * @param o
	 * 		value
	 */
	public void setValue(int i, Object o)			
	{	values[i] = o; 
	}

	/**
	 * Gets a value at given index as a string.
	 * 
	 * @param i
	 * 		index of value
	 * @return
	 * 		String value
	 */
	public String getString(int i) 					
	{	return (String) values[i]; 
	}

	/**
	 * Gets a value at given index as an object.
	 * 
	 * @param i
	 * 		index of value
	 * @return
	 * 		Object value
	 */
	public Object getObject(int i) 					
	{	return values[i]; 
	}
	
	/**
	 * Gets a value at given index as an int.
	 * 
	 * @param i
	 * 		index of value
	 * @return
	 * 		int value
	 */
	public int getInt(int i)
	{	if (values[i].getClass() == java.lang.String.class)
			return Integer.parseInt((String) values[i]);
		else
			return ((Integer) values[i]).intValue();
	}


	/*
	 *  I/O Methods
	 */
	
	/**
	 * Reads a tuple in text form from given reader.
	 * 
	 * @param in
	 * 		input to read from
	 * @return
	 * 		true if success, false if failure
	 * @throws IOException
	 * 		if an I/O error occurs
	 */
	public boolean readText(BufferedReader in) throws IOException
	{	String st = in.readLine();
		if (st == null)
			return false;

		StringTokenizer myTokenizer = new StringTokenizer(st);
		int numVals = myTokenizer.countTokens();

		values = new Object[numVals];

		for (int i=0; i < numVals; i++)
		{	String val = myTokenizer.nextToken();

				int attrType = relation.getAttributeType(i);

				if (attrType == Attribute.TYPE_INT)
					values[i] = Integer.valueOf(Integer.parseInt((String) val));
				else
					values[i] = val; 					// type string by default
		}
		return true;
	}

	/**
	 * Writes a tuple in text form to an output writer.
	 * 
	 * @param out
	 * 		output writer
	 */
	public void writeText(PrintWriter out)
	{	for (int i=0; i < numValues(); i++)
			out.print(getObject(i)+"\t");
		out.println();
	}

	/**
	 * Reads a tuple in binary form from a stream.
	 * 
	 * @param in
	 * 		binary input stream
	 * 
	 * @return
	 * 		true if success, false if failure
	 * @throws IOException
	 * 		if an I/O error occurs
	 */
	public boolean read(BufferedInputStream in) throws IOException
	{	// Read the tuple from disk into memory

		int header;
		byte []tmp = new byte[1000];

		header = in.read();							// Read record header
		if (header == -1)
			return false;

		in.read(tmp, 0, 4);							// Read record length

		int numFields = Convert.toInt(tmp);
		short []offsets = new short[numFields];
		boolean [] isNull = new boolean[numFields];

		// Read the NULL bit array and offsets
		for (int i=0; i < numFields; i++)
			isNull[i] = (in.read() > 0);

		for (int i=0; i < numFields; i++)
		{	in.read(tmp, 0, 2);				// Read short offset
			offsets[i] = Convert.toShort(tmp);
		}

		// Read the data
		for (int i=0; i < numFields; i++)
			values[i] = Attribute.read(in, relation.getAttributeType(i));

		return true;
	}

	/**
	 * Writes a tuple in binary form to an output stream.
	 * 
	 * @param out
	 * 		output stream
	 */
	public void write(BufferedOutputStream out) throws IOException
	{	byte b = 1;
		short []offsets = new short[values.length];
		boolean [] isNull = new boolean[values.length];		
		int curPos = 0;

		// Determine field offsets
		for (int i=0; i < values.length; i++)
		{	if (values[i] == null)
				isNull[i] = true;
			else
			{	isNull[i] = false;
				offsets[i] = (short) curPos;

				// Put into correct number of bytes - doing only int and string for now				
				int attrType = relation.getAttributeType(i);
				int size = Attribute.getByteSize(attrType, values[i]);

				curPos = curPos +  size;
			}
		}


		// Write out the record
		out.write(b);									// Record header - not currently used
		out.write(Convert.toByte(values.length));		// Number of fields
		for (int i=0; i < values.length; i++)			// Write isNULL bit array
			if (isNull[i])
				out.write(1);
			else
				out.write(0);

		for (int i=0; i < values.length; i++)			// Write field offsets
			out.write(Convert.toByte(offsets[i]));

		for (int i=0; i < values.length; i++)
			if (!isNull[i])
				Attribute.write(out, relation.getAttributeType(i), values[i]);
	}

	@Override
	public String toString()
	{	String st = "";
		for (int i=0; i < numValues(); i++)
			st = st + getObject(i)+"\t";
		return st;
	}
}
