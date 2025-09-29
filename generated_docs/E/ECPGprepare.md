# ECPGprepare

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:217-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L217-L238)

## Overview
The main public API function for handling EXEC SQL PREPARE statements in the ECPG library, providing a high-level interface for preparing SQL statements.

## Definition

```c
struct connection *con;
```
## Detailed Description
The `ECPGprepare` function is the primary entry point for preparing SQL statements in ECPG applications. It handles the complete process of statement preparation including connection management, duplicate statement checking, and delegation to the core preparation logic. The function first retrieves and initializes the specified database connection, then checks for existing prepared statements with the same name and deallocates them if found, and finally delegates to `prepare_common` for the actual preparation work.

## Parameters / Member Variables
- `lineno`: Line number for error reporting and debugging purposes
- `connection_name`: Name of the database connection to use for preparing the statement
- `questionmarks`: Legacy parameter for API compatibility (currently unused)
- `name`: The name to assign to the prepared statement for later reference
- `variable`: The SQL command text to be prepared

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_get_connection](../e/ecpg_get_connection.md)
  - [ecpg_init](../e/ecpg_init.md)
  - [ecpg_find_prepared_statement](../e/ecpg_find_prepared_statement.md)
  - [deallocate_one](../d/deallocate_one.md)
  - [prepare_common](../p/prepare_common.md)
- Structures used:
  - [connection](../c/connection.md)
  - [prepared_statement](../p/prepared_statement.md)
- Constants used:
  - ECPG_COMPAT_PGSQL
- Called from (representative examples):
  - [ecpg_auto_prepare](../e/ecpg_auto_prepare.md)
  - Various test functions and user applications

## Notes and Other Information
- This is a public API function exposed in ecpglib.h
- Returns true on success, false on failure
- The questionmarks parameter is kept for API compatibility but is not used
- Automatically handles connection retrieval and initialization
- Prevents duplicate prepared statement names by deallocating existing ones
- Widely used throughout ECPG test suites and real applications
- Part of the ECPG library's public interface for prepared statement management
- Delegates the actual preparation work to prepare_common after handling connection and duplicate checking logic

## Simplified Source

```c
bool ECPGprepare(int lineno, const char *connection_name, const bool questionmarks,
                 const char *name, const char *variable)
{
    struct connection *con;
    struct prepared_statement *this, *prev;

    // questionmarks parameter is kept for API compatibility but unused
    (void) questionmarks;

    // Get and initialize the database connection
    con = ecpg_get_connection(connection_name);
    if (!ecpg_init(con, connection_name, lineno))
        return false;

    // Check if a prepared statement with this name already exists
    this = ecpg_find_prepared_statement(name, con, &prev);
    if (this && !deallocate_one(lineno, ECPG_COMPAT_PGSQL, con, prev, this))
        return false;

    // Delegate to common preparation logic
    return prepare_common(lineno, con, name, variable);
}
```