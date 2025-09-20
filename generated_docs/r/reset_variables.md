# reset_variables

## Location
[src/interfaces/ecpg/preproc/variable.c:367-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L367-L376)

## Overview
Resets the global variables that track argument lists for SQL statement parameters, clearing both the insert and result argument lists.

## Definition

```c
void
reset_variables(void)
```
## Detailed Description
The  function initializes the global argument list variables  and  to NULL. These global variables are used by the ECPG preprocessor to maintain lists of variables that serve as parameters for SQL INSERT statements () and variables that receive results from SQL queries (). This function is typically called to clean up or reset the state between processing different SQL statements.

The function is part of PostgreSQL's Embedded SQL (ECPG) preprocessor, which translates embedded SQL statements in C programs into appropriate PostgreSQL libpq calls.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - None (only sets global variables to NULL)
- Called from (representative examples):
  -  (from src/interfaces/ecpg/preproc/output.c:163)

## Notes and Other Information
- The global variables  and  are declared as  in preproc_extern.h
- These variables are used throughout the ECPG preprocessor to build argument lists for SQL statements
- The function provides a clean slate for processing new SQL statements by clearing any previously accumulated argument lists
- This is a simple utility function with no error handling, as setting pointers to NULL is a safe operation