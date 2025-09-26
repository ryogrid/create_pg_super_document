# test

## Location
[src/interfaces/ecpg/test/expected/preproc-autoprep.c:26-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-autoprep.c#L26-L250)

## Overview
The  function is a static test function in the ECPG (Embedded SQL in C for PostgreSQL) test suite that demonstrates various SQL operations including auto-prepared statements, cursor handling, and database connectivity.

## Definition

```c
#line 6 "autoprep.pgc"


static void test(void)
```
## Detailed Description
This function serves as a comprehensive test case for ECPG functionality, specifically for auto-prepared statements. It performs a series of database operations including:
- Establishing a database connection
- Creating and populating a test table
- Executing various SQL statements with auto-preparation
- Using cursors for data retrieval
- Handling prepared statements
- Managing database cleanup

The function tests core ECPG features such as embedded SQL execution, parameter binding, cursor operations, and proper error handling through sqlca (SQL Communication Area) status checking.

## Parameters / Member Variables
This function takes no parameters as it is a void function designed for testing purposes.

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGdebug](../E/ECPGdebug.md): Enables ECPG debugging output
  - [ECPGconnect](../E/ECPGconnect.md): Establishes database connection
  - [ECPGdo](../E/ECPGdo.md): Executes SQL statements
  - [ECPGprepare](../E/ECPGprepare.md): Prepares SQL statements
  - [ECPGprepared_statement](../E/ECPGprepared_statement.md): References prepared statements
  - [ECPGdisconnect](../E/ECPGdisconnect.md): Closes database connection
  - [sqlprint](../s/sqlprint.md): Prints SQL error/warning messages
  - printf: Standard C library function for output

- Called from (representative examples):
  - [main](../m/main.md): Main function in the same test file (src/interfaces/ecpg/test/expected/preproc-autoprep.c:252)

## Notes and Other Information
- This is a generated test file from the ECPG preprocessor, as indicated by the .c extension and the presence of preprocessor line directives
- The function includes comprehensive error handling using sqlca.sqlwarn and sqlca.sqlcode
- It demonstrates both immediate execution and prepared statement execution patterns
- The test creates temporary data structures and cleans them up properly
- This function is part of the PostgreSQL regression test suite for ECPG functionality