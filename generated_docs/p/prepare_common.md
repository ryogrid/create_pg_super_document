# prepare_common

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:159-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L159-L216)

## Overview
A static function that handles the common preparation logic for SQL statements in the ECPG library, creating prepared statements and registering them with PostgreSQL.

## Definition

```c
static bool
prepare_common(int lineno, struct connection *con, const char *name, const char *variable)
```
## Detailed Description
The `prepare_common` function encapsulates the core logic for preparing SQL statements in ECPG. It allocates memory for prepared statement and statement structures, processes the SQL text to replace named parameters with positional parameters, sends the prepared statement to PostgreSQL using PQprepare, and registers the prepared statement in the connection's list. The function handles error conditions gracefully by cleaning up allocated resources on failure.

## Parameters / Member Variables
- `lineno`: Line number for error reporting and memory allocation tracking
- `con`: Pointer to the database connection structure where the prepared statement will be registered
- `name`: The name to assign to the prepared statement
- `variable`: The SQL command text to be prepared

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_alloc
  - ecpg_free
  - ecpg_strdup
  - replace_variables
  - PQprepare
  - ecpg_check_PQresult
  - ecpg_log
  - PQclear
- Structures used:
  - statement
  - prepared_statement
  - connection
- Called from (representative examples):
  - ECPGprepare
  - ecpg_auto_prepare

## Notes and Other Information
- This is a static function local to the prepare.c file in the ECPG library
- Returns true on success, false on failure
- Performs comprehensive error handling with proper resource cleanup
- Uses replace_variables to transform named parameters to PostgreSQL's positional parameter format
- Calls PQprepare to actually prepare the statement on the PostgreSQL server
- Validates the preparation result using ecpg_check_PQresult
- Logs the preparation operation for debugging purposes
- Maintains a linked list of prepared statements per connection
- Sets the prepared flag to true after successful preparation
- Part of the ECPG library's prepared statement infrastructure