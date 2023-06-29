[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-c66648af7eb3fe8bc4f294546bfd86ef473780cde1dea487d3c4ff354943c9ae.svg)](https://classroom.github.com/online_ide?assignment_repo_id=10022018&assignment_repo_type=AssignmentRepo)
# COSC 404 - Database System Implementation<br>Lab 4 - Query Processing with Iterators

This lab practices programming with iterators.

## Programming Iterators (20 marks)

Implement three relational algebra operators as iterators.  You are not required to edit most of the provided code files. **Note that there is a `FileManager.java` for opening and closing files, and `Tuple.java` has methods for reading and writing tuples from files.  Make use of these files!** The three operators to implement:

- In `TextFileScan.java` complete `init()`, `next()`, and `close()` methods to create an operator called `TextFileScan` to scan a text file as a set of tuples. (Test file: `TestScan.java`) *(5 marks)*

- In `NestedLoopJoin.java` write the code to implement a tuple nested-loop join.  The general form of the iterator is in the query processing notes. (Test file: `TestNestedLoopJoin.java`) *(5 marks)*

- In `MergeJoin.java`, implement the merge portion of a sort join (assume the inputs are already sorted). Assume that there cannot exist duplicate keys in either input (e.g. two 1's in left input). The tiny and small tests will pass as they have no duplicates. (Test file: `TestMergeJoin.java`) (10 marks) 

- BONUS: For `MergeJoin`, implement a strategy to buffer input from the left side if there are multiple tuples with the same key on the right side in order to complete all the joins. Do not buffer the whole file. Only buffer records that have the same join key. (Test file: `TestMergeJoinBonus.java`) (10 marks)

## Submission

The lab can be marked immediately by the professor or TA by showing the output of the JUnit tests and by a quick code review.  Submit the URL of your GitHub repository on Canvas. **Make sure to commit and push your updates to GitHub.**
