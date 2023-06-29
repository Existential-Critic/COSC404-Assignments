[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-c66648af7eb3fe8bc4f294546bfd86ef473780cde1dea487d3c4ff354943c9ae.svg)](https://classroom.github.com/online_ide?assignment_repo_id=9733853&assignment_repo_type=AssignmentRepo)
# COSC 404 - Database System Implementation<br/>Lab 1 - MySQL vs. PostgreSQL - Creating and Querying Data

This lab practices creating and querying data on MySQL and PostgreSQL and allows comparing the two database systems.

## Setup

The labs use Java, Docker, and GitHub. Follow the [setup and installation instructions](https://github.com/rlawrenc/cosc_404/tree/main/labs/setup) to get your computer setup for the labs.

## MySQL

One of the Docker containers is running MySQL 8. Connection information is in the `docker-compose.yml` file.

### References:

- [MySQL Date Time Functions](http://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html)
- [MySQL String Functions](http://dev.mysql.com/doc/refman/8.0/en/string-functions.html)
- [MySQL Auto_Increment](http://dev.mysql.com/doc/refman/8.0/en/example-auto-increment.html)
- [MySQL Date and Time Types](http://dev.mysql.com/doc/refman/8.0/en/datetime.html)


## PostgreSQL

One of the Docker containers is running Postgres 14. Connection information is in the `docker-compose.yml` file.

### References:

- [PostgreSQL Date Functions](http://www.postgresql.org/docs/14/static/functions-datetime.html)
- [PostgreSQL String Functions](http://www.postgresql.org/docs/14/static/functions-string.html)
- [PostgreSQL Create Table Syntax](http://www.postgresql.org/docs/14/static/sql-createtable.html)

## Tasks

Starter code is available including JUnit test cases and the JDBC drivers for MySQL and PostgreSQL. 

You will repeat the same tasks for both PostgreSQL and MySQL.  The goal is to build experience with both systems and see their similarities and differences.  Expect to consult the online documentation for MySQL and PostgreSQL to help with this assignment. **Hint: Complete one of the databases first then copy the code to the other.  There are only a few changes between the two.**

The two classes you will write are `QueryMySQL.java` and `QueryPostgreSQL.java`.  The test classes are `TestQueryMySQL.java` and `TestQueryPostgreSQL.java` respectively.  You will fill in the methods requested (search for **TODO**).  Marks for each method are below.  You receive the marks if you pass the JUnit tests AND have followed the requirements asked in the question (including documentation and proper formatting).

- +1 mark - Method `connect()` to make a connection to the database.
- +1 mark - Method `close()` to close the connection to the database.
- +1 mark - Method `drop()` to drop the table "person" that we will be using.
- +2 marks - Method `create()` to create a "person" table with fields:
  	- id - integer, must auto-increment
	- name - variable character field up to size 40
	- salary - must hold up to 99,999,999.99 exactly
	- birthdate - date
	- last_update - timestamp	
- +3 marks - Method `insert()` to add the following records.  **You must use PreparedStatements to get full marks.**	

```
name, salary, birthdate, last_update
1, Ann Alden, 123000.00, 1986-03-04, 2022-01-04 11:30:30.0
2, Bob Baron, 225423.00, 1993-12-02, 2022-01-04 12:30:25.0
3, Chloe Cat, 99999999.99, 1999-01-15, 2022-01-04 12:25:45.0
4, Don Denton, 91234.24, 2004-08-03, 2022-01-04 12:45:00.0
5, Eddy Edwards, 55125125.25, 2003-05-17, 2022-01-05 23:00:00.0
```

- +1 mark - Write the method `delete()` to delete the person with name `'Bob Baron'`.</li>
- +2 marks - Write the method `query1()` that returns the person name and salary where rows are sorted by salary descending.</li>
- +2 marks - Write the method `query2()` that returns the person's last name and salary if the person's salary is greater than the average salary of all people.</li>
- +2 marks - Write the method `query3()` that returns all fields of a pair of people where a pair of people is returned if the last_update field of their records have been updated less than an hour apart. Do not duplicate pairs.  Example: Only show (Ann, Bob) and not also (Bob, Ann).</li>

**Total Marks: 30** (15 marks for each database)

## Submission

The lab can be marked immediately by the professor or TA by showing the output of the JUnit tests and by a quick code review.  Otherwise, submit the URL of your GitHub repository on Canvas. **Make sure to commit and push your updates to GitHub.**

