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
  - [ecpg_alloc](../e/ecpg_alloc.md)
  - [ecpg_free](../e/ecpg_free.md)
  - [ecpg_strdup](../e/ecpg_strdup.md)
  - [replace_variables](../r/replace_variables.md)
  - [PQprepare](../P/PQprepare.md)
  - [ecpg_check_PQresult](../e/ecpg_check_PQresult.md)
  - [ecpg_log](../e/ecpg_log.md)
  - [PQclear](../P/PQclear.md)
- Structures used:
  - [statement](../s/statement.md)
  - [prepared_statement](prepared_statement.md)
  - [connection](../c/connection.md)
- Called from (representative examples):
  - [ECPGprepare](../E/ECPGprepare.md)
  - [ecpg_auto_prepare](../e/ecpg_auto_prepare.md)

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

## Simplified Source

```c
static bool prepare_common(int lineno, struct connection *con, const char *name, const char *variable) {
    // Allocate new prepared statement structure
    struct prepared_statement *this = (struct prepared_statement *)
        ecpg_alloc(sizeof(struct prepared_statement), lineno);
    if (!this)
        return false;

    // Allocate statement structure
    struct statement *stmt = (struct statement *)
        ecpg_alloc(sizeof(struct statement), lineno);
    if (!stmt) {
        ecpg_free(this);
        return false;
    }

    // Initialize statement
    stmt->lineno = lineno;
    stmt->connection = con;
    stmt->command = ecpg_strdup(variable, lineno);
    stmt->inlist = stmt->outlist = NULL;

    // Replace C variables with parameter placeholders
    replace_variables(&(stmt->command), lineno);

    // Set up prepared statement
    this->name = ecpg_strdup(name, lineno);
    this->stmt = stmt;

    // Send PREPARE command to PostgreSQL server
    PGresult *query = PQprepare(stmt->connection->connection, name, stmt->command, 0, NULL);
    if (!ecpg_check_PQresult(query, stmt->lineno, stmt->connection->connection, stmt->compat)) {
        // Cleanup on failure
        ecpg_free(stmt->command);
        ecpg_free(this->name);
        ecpg_free(this);
        ecpg_free(stmt);
        return false;
    }

    // Log successful preparation
    ecpg_log("prepare_common on line %d: name %s; query: \"%s\"\n",
             stmt->lineno, name, stmt->command);
    PQclear(query);
    this->prepared = true;

    // Add to connection's prepared statement list
    this->next = con->prep_stmts;
    con->prep_stmts = this;

    return true;
}
```