[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-c66648af7eb3fe8bc4f294546bfd86ef473780cde1dea487d3c4ff354943c9ae.svg)](https://classroom.github.com/online_ide?assignment_repo_id=10230254&assignment_repo_type=AssignmentRepo)
# COSC 404 - Database System Implementation<br/>Lab 6 - Storing JSON Documents: MongoDB and PostgreSQL

This lab practices using JSON data in both MongoDB and PostgreSQL.

## Using MongoDB (10 marks)

[MongoDB](https://www.mongodb.org/) is a NoSQL system designed for large data sets with convenient support for storing JSON documents. MongoDB targets high performance, high availability, and scability.  It has a query API that is different than SQL and works with many common languages including Java.  This question asks you to load data into MongoDB, update a document, and perform queries using the Java query API for MongoDB.  Modify `QueryMongo.java` to perform the tasks and test with the JUnit test file `TestMongo.java`.

- +3 marks - Method `load()` to load data into the database.
- +2 marks - Method `update()` to update one document.
- +2 marks - Method `query1()` to perform basic query with filter and projection.
- +3 marks - Method `query2()` to perform query with OR and arrays.

### References

- [MongoDB Introduction](https://docs.mongodb.org/manual/core/introduction/)
- [Creating a Collection](https://docs.mongodb.org/manual/reference/method/db.createCollection/)
- [Modifying Documents](https://docs.mongodb.org/manual/tutorial/modify-documents/)
- [Querying using find()](https://docs.mongodb.org/manual/reference/method/db.collection.find/)
- [Mongo Java Driver API](https://mongodb.github.io/mongo-java-driver/4.4/apidocs/mongodb-driver-core/com/mongodb/package-summary.html)


## Using PostgreSQL with JSON Data (10 bonus marks)

[PostgreSQL](https://www.postgresql.org/) is also capable of storing JSON data and manipulating it using SQL. In this bonus question, modify `QueryPostgres.java` to perform the same insert of JSON data, update, and querying as done with MongoDB in question #1. The JUnit test file is `TestPostgres.java`.


- +3 marks - Method `load()` to load data into the database *using PreparedStatements*.
- +2 marks - Method `update()` to update one document.
- +2 marks - Method `query1()` to perform basic query with filter and projection.
- +3 marks - Method `query2()` to perform query with OR and arrays.


### References

- [PostgreSQL JSON data type](https://www.postgresql.org/docs/14/static/datatype-json.html)
- [PostgreSQL JSON Functions and Operators](https://www.postgresql.org/docs/14/static/functions-json.html)

## Submission

The lab can be marked immediately by the professor or TA by showing the output of the JUnit tests and by a quick code review.  Otherwise, submit the URL of your GitHub repository on Canvas. **Make sure to commit and push your updates to GitHub.**
