package textdb;

import java.sql.Types;
import java.math.BigDecimal;
import java.io.*;

/**
 * Represents a relational attribute.
 */
public class Attribute {
	private String name;
	private int type;
	private int length;

	// Static constants for types
	public static int TYPE_SMALLINT = Types.SMALLINT;
	public static int TYPE_INT = Types.INTEGER;
	public static int TYPE_FLOAT = Types.FLOAT;
	public static int TYPE_DECIMAL = Types.DECIMAL;
	public static int TYPE_DOUBLE = Types.DOUBLE;
	public static int TYPE_CHAR = Types.CHAR;
	public static int TYPE_STRING = Types.VARCHAR;
	public static int TYPE_BLOB = Types.BLOB;
	public static int TYPE_DATE = Types.DATE;
	public static int TYPE_TIMESTAMP = Types.TIMESTAMP;

	public Attribute() {
		name = "";
		type = 0;
	}

	public Attribute(Attribute a) {
		name = a.name;
		type = a.type;
		length = a.length;
	}

	public Attribute(String n, int t, int l) {
		name = n;
		type = t;
		length = l;
	}

	// Get/Set Methods
	public String getName() {
		return name;
	}

	public void setName(String st) {
		name = st;
	}

	public int getType() {
		return type;
	}

	public void setType(int t) {
		type = t;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int l) {
		length = l;
	}

	// I/O Methods
	public static Object read(BufferedInputStream in, int attrType) throws IOException {
		byte[] tmp = new byte[1000];

		if (attrType == Attribute.TYPE_INT) {
			in.read(tmp, 0, 4);
			return Integer.valueOf(Convert.toInt(tmp));
		} else if (attrType == Attribute.TYPE_SMALLINT) {
			in.read(tmp, 0, 4);
			return Short.valueOf(Convert.toShort(tmp));
		} else if (attrType == Attribute.TYPE_DECIMAL) {
			byte len = (byte) in.read();
			in.read(tmp, 0, len);
			tmp[len] = 0;
			return Convert.toBigDecimal(tmp, len);
		} else if (attrType == Attribute.TYPE_STRING || attrType == Attribute.TYPE_CHAR) {
			byte len = (byte) in.read();
			in.read(tmp, 0, len);
			tmp[len] = 0;
			return Convert.toString(tmp, len);
		} else if (attrType == Attribute.TYPE_TIMESTAMP) { // Type is java.sql.Timestamp
			byte len = (byte) in.read();
			in.read(tmp, 0, len);
			tmp[len] = 0;
			return java.sql.Timestamp.valueOf(Convert.toString(tmp, len));
		}

		return null;
	}

	public static void write(BufferedOutputStream out, int attrType, Object obj) throws IOException {
		byte[] raw = null;

		if (attrType == Attribute.TYPE_INT) {
			raw = Convert.toByte(((Integer) obj).intValue());
		} else if (attrType == Attribute.TYPE_SMALLINT) {
			raw = Convert.toByte(((Short) obj).intValue());
		} else if (attrType == Attribute.TYPE_DECIMAL) {
			raw = Convert.toByte(((BigDecimal) obj).toString());
			out.write(raw.length); // Write out size of decimal string
		} else if (attrType == Attribute.TYPE_STRING || attrType == Attribute.TYPE_CHAR) {
			raw = Convert.toByte((String) obj);
			out.write(raw.length); // Write out size of string
		} else if (attrType == Attribute.TYPE_DATE) {
			System.out.println("Date type.");
		} else if (attrType == Attribute.TYPE_TIMESTAMP) {
			raw = Convert.toByte(obj.toString());
			out.write(raw.length);
		}

		if (raw == null) {
			System.out.println("Type: " + attrType + " Data: " + obj);
		}
		out.write(raw, 0, raw.length);
	}

	// Other Methods
	public static int getByteSize(int attrType, Object obj) {
		if (attrType == Attribute.TYPE_INT)
			return 4;
		else if (attrType == Attribute.TYPE_SMALLINT)
			return 2;
		else if (attrType == Attribute.TYPE_DECIMAL) // Treat as a character string
			return (((BigDecimal) obj).toString()).length() + 1;
		else if (attrType == Attribute.TYPE_STRING)
			return ((String) obj).length() + 1;

		return 0;
	}

	public String toString() {
		return name + ":" + type + "(" + length + ")";
	}
}
