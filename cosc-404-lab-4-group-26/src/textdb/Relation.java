package textdb;

/**
 * Represents a relational schema.
 */
public class Relation {
	private Attribute[] attributes;

	public Relation() {
		attributes = null;
	}

	public Relation(Attribute[] attrs) {
		attributes = attrs;
	}

	public Relation(Relation r) {
		attributes = new Attribute[r.attributes.length];
		for (int i = 0; i < attributes.length; i++)
			attributes[i] = new Attribute(r.attributes[i]);
	}

	public int getAttributeType(int index) {
		return attributes[index].getType();
	}

	public int getNumAttributes() {
		return attributes.length;
	}

	public Attribute getAttribute(int index) {
		return attributes[index];
	}

	public void mergeRelation(Relation r) {
		if (attributes == null) {
			attributes = new Attribute[r.attributes.length];

			for (int i = 0; i < r.attributes.length; i++)
				attributes[i] = r.attributes[i];
		} else {
			int len = attributes.length + r.attributes.length;
			Attribute[] attr = new Attribute[len];

			for (int i = 0; i < attributes.length; i++)
				attr[i] = attributes[i];

			for (int i = attributes.length; i < len; i++)
				attr[i] = r.attributes[i - attributes.length];

			attributes = attr;
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < attributes.length; i++) {
			sb.append(attributes[i].toString());
			sb.append(" ");
		}
		return sb.toString();
	}
}
