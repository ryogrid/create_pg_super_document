# errdetail_execute

## Location
[src/backend/tcop/postgres.c:2470-2502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2470-L2502)

## Overview
Adds an error detail line showing the original query text referenced by an EXECUTE statement when errors occur during query execution.

## Definition

```c
static int
errdetail_execute(List *raw_parsetree_list)
```
## Detailed Description
This function searches through a list of raw parse trees to find EXECUTE statements and provides additional error context by showing the original prepared statement query text. When an error occurs during execution of a prepared statement, this function enhances the error message by including the actual SQL query that was prepared, making debugging easier for users who see EXECUTE statement errors.

The function iterates through the provided parse tree list, identifies EXECUTE statements, fetches the corresponding prepared statement, and adds the original query string as an error detail using the errdetail() function.

## Parameters / Member Variables
- : List of RawStmt nodes representing the parsed SQL statements to examine for EXECUTE statements

## Dependencies
- Functions called/Symbols referenced:
  - RawStmt (parse tree node structure)
  - ExecuteStmt (EXECUTE statement node)
  - PreparedStatement (prepared statement structure)
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) (retrieves prepared statement by name)
  - [errdetail](errdetail.md) (adds detail to error messages)
- Called from (representative examples):
  - [exec_simple_query](exec_simple_query.md) (when handling query execution errors)

## Notes and Other Information
- Returns 0 in all cases (return value appears to be unused)
- Only processes the first EXECUTE statement found in the parse tree list
- Safely handles cases where the prepared statement cannot be found
- Used specifically for error reporting to provide better context to users
- Part of PostgreSQL's error handling and reporting system