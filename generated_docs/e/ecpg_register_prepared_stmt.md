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
  - ecpg_find_prepared_statement
  - deallocate_one
  - ecpg_alloc
  - ecpg_free
  - ecpg_strdup
  - memset
- Structures used:
  - statement
  - prepared_statement
  - connection
- Constants used:
  - ECPG_COMPAT_PGSQL
- Called from (representative examples):
  - ecpg_execute

## Notes and Other Information
- Returns true on success, false on failure
- Handles memory allocation failures gracefully by cleaning up allocated resources
- Maintains a linked list of prepared statements per connection
- Automatically deallocates existing prepared statements with the same name before creating a new one
- The function creates a deep copy of the statement command and name using ecpg_strdup
- Sets the prepared flag to true in the prepared statement structure
- Part of the ECPG library's prepared statement management system