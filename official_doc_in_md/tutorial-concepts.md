2.2. Concepts  
---  
[Prev](tutorial-sql-intro.md "2.1. Introduction") | [Up](tutorial-sql.md "Chapter 2. The SQL Language")| Chapter 2. The SQL Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](tutorial-table.md "2.3. Creating a New Table")  
  
* * *

## 2.2. Concepts #

PostgreSQL is a _relational database management system_ (RDBMS). That means it is a system for managing data stored in _relations_. Relation is essentially a mathematical term for _table_. The notion of storing data in tables is so commonplace today that it might seem inherently obvious, but there are a number of other ways of organizing databases. Files and directories on Unix-like operating systems form an example of a hierarchical database. A more modern development is the object-oriented database. 

Each table is a named collection of _rows_. Each row of a given table has the same set of named _columns_ , and each column is of a specific data type. Whereas columns have a fixed order in each row, it is important to remember that SQL does not guarantee the order of the rows within the table in any way (although they can be explicitly sorted for display). 

Tables are grouped into databases, and a collection of databases managed by a single PostgreSQL server instance constitutes a database _cluster_. 

* * *

[Prev](tutorial-sql-intro.md "2.1. Introduction") | [Up](tutorial-sql.md "Chapter 2. The SQL Language")|  [Next](tutorial-table.md "2.3. Creating a New Table")  
---|---|---  
2.1. Introduction | [Home](index.md "PostgreSQL 17.5 Documentation")|  2.3. Creating a New Table
