# ecpg_register_prepared_stmt

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:59-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L59-L103)

## Overview
Registers a prepared statement in the ECPG library, managing the lifecycle of prepared statements within a database connection.

## Definition

```c
bool
ecpg_register_prepared_stmt(struct statement *stmt)
```
## Detailed Description
The `ecpg_register_prepared_stmt` function creates and registers a new prepared statement in the ECPG system. It first checks if a statement with the same name already exists and deallocates it if found. Then it allocates memory for both a new prepared statement structure and its associated statement structure, initializes them with the provided data, and adds the prepared statement to the connection's list of prepared statements.

## Parameters / Member Variables
- `stmt`: A pointer to the statement structure containing the information needed to create the prepared statement, including name, command, and connection details

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_find_prepared_statement](ecpg_find_prepared_statement.md)
  - [deallocate_one](../d/deallocate_one.md)
  - [ecpg_alloc](ecpg_alloc.md)
  - [ecpg_free](ecpg_free.md)
  - [ecpg_strdup](ecpg_strdup.md)
  - memset
- Structures used:
  - [statement](../s/statement.md)
  - [prepared_statement](../p/prepared_statement.md)
  - [connection](../c/connection.md)
- Constants used:
  - ECPG_COMPAT_PGSQL
- Called from (representative examples):
  - [ecpg_execute](ecpg_execute.md)

## Notes and Other Information
- Returns true on success, false on failure
- Handles memory allocation failures gracefully by cleaning up allocated resources
- Maintains a linked list of prepared statements per connection
- Automatically deallocates existing prepared statements with the same name before creating a new one
- The function creates a deep copy of the statement command and name using ecpg_strdup
- Sets the prepared flag to true in the prepared statement structure
- Part of the ECPG library's prepared statement management system

## Simplified Source

```c
bool ecpg_register_prepared_stmt(struct statement *stmt) {
    struct statement *prep_stmt;
    struct prepared_statement *this;
    struct connection *con = stmt->connection;
    struct prepared_statement *prev = NULL;
    int lineno = stmt->lineno;

    // Check if statement already exists and remove it
    this = ecpg_find_prepared_statement(stmt->name, con, &prev);
    if (this && !deallocate_one(lineno, ECPG_COMPAT_PGSQL, con, prev, this))
        return false;

    // Allocate new prepared statement structure
    this = (struct prepared_statement *) ecpg_alloc(sizeof(struct prepared_statement), lineno);
    if (!this)
        return false;

    // Allocate new statement structure
    prep_stmt = (struct statement *) ecpg_alloc(sizeof(struct statement), lineno);
    if (!prep_stmt) {
        ecpg_free(this);
        return false;
    }
    memset(prep_stmt, 0, sizeof(struct statement));

    // Initialize statement structure
    prep_stmt->lineno = lineno;
    prep_stmt->connection = con;
    prep_stmt->command = ecpg_strdup(stmt->command, lineno);
    prep_stmt->inlist = prep_stmt->outlist = NULL;

    // Initialize prepared statement structure
    this->name = ecpg_strdup(stmt->name, lineno);
    this->stmt = prep_stmt;
    this->prepared = true;

    // Add to connection's prepared statement list
    this->next = con->prep_stmts;
    con->prep_stmts = this;

    return true;
}
```