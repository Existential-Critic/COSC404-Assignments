/* Generated By:JJTree: Do not edit this line. ASTMyID.java */
package textdb.parser;

public class ASTMyID extends SimpleNode {
  public ASTMyID(int id) {
    super(id);
  }

  public ASTMyID(SimpleParser p, int id) {
    super(p, id);
  }

  public void setName(String n) {
    name = n;
  }

  public String toString() {
    return "Identifier: "+name;
  }

  private String name;

}