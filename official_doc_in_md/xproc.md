36.4. User-Defined Procedures  
---  
[Prev](xfunc.md "36.3. User-Defined Functions") | [Up](extend.md "Chapter 36. Extending SQL")| Chapter 36. Extending SQL| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](xfunc-sql.md "36.5. Query Language \(SQL\) Functions")  
  
* * *

## 36.4. User-Defined Procedures #

A procedure is a database object similar to a function. The key differences are: 

  * Procedures are defined with the [`CREATE PROCEDURE`](sql-createprocedure.md "CREATE PROCEDURE") command, not `CREATE FUNCTION`. 

  * Procedures do not return a function value; hence `CREATE PROCEDURE` lacks a `RETURNS` clause. However, procedures can instead return data to their callers via output parameters. 

  * While a function is called as part of a query or DML command, a procedure is called in isolation using the [`CALL`](sql-call.md "CALL") command. 

  * A procedure can commit or roll back transactions during its execution (then automatically beginning a new transaction), so long as the invoking `CALL` command is not part of an explicit transaction block. A function cannot do that. 

  * Certain function attributes, such as strictness, don't apply to procedures. Those attributes control how the function is used in a query, which isn't relevant to procedures. 




The explanations in the following sections about how to define user-defined functions apply to procedures as well, except for the points made above. 

Collectively, functions and procedures are also known as _routines_. There are commands such as [`ALTER ROUTINE`](sql-alterroutine.md "ALTER ROUTINE") and [`DROP ROUTINE`](sql-droproutine.md "DROP ROUTINE") that can operate on functions and procedures without having to know which kind it is. Note, however, that there is no `CREATE ROUTINE` command. 

* * *

[Prev](xfunc.md "36.3. User-Defined Functions") | [Up](extend.md "Chapter 36. Extending SQL")|  [Next](xfunc-sql.md "36.5. Query Language \(SQL\) Functions")  
---|---|---  
36.3. User-Defined Functions | [Home](index.md "PostgreSQL 17.5 Documentation")|  36.5. Query Language (SQL) Functions
