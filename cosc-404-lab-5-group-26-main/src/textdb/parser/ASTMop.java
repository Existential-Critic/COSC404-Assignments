/* Generated By:JJTree: Do not edit this line. ASTMop.java */
package textdb.parser;

public class ASTMop extends SimpleNode {
  public ASTMop(int id) {
    super(id);
  }

  public ASTMop(SimpleParser p, int id) {
    super(p, id);
  }

  public void setName(String n) {
    name = n;
  }

  public String toString() {
    return "Mult_Op: "+name;
  }

  private String name;

}