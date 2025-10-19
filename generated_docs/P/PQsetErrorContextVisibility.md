# PQsetErrorContextVisibility

## Location
[src/interfaces/libpq/fe-connect.c:7309-7320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7309-L7320)

## Overview
Controls the visibility of context information in error messages, allowing applications to specify when context details should be included in error reports.

## Definition

```c
PGContextVisibility
PQsetErrorContextVisibility(PGconn *conn, PGContextVisibility show_context)
```
## Detailed Description
PQsetErrorContextVisibility configures whether and when context information is displayed in error messages from the PostgreSQL client library. Context information provides additional details about where an error occurred, such as function call stacks or query parsing context. The function updates the connection's context visibility setting and returns the previous setting, enabling temporary changes to context reporting behavior.

## Parameters / Member Variables
- : The database connection handle (if NULL, returns PQSHOW_CONTEXT_ERRORS)
- : The desired context visibility level from the PGContextVisibility enum:
  - : Never show CONTEXT field in error messages
  - : Show CONTEXT for errors only (default behavior)
  - : Always show CONTEXT field in all messages

## Dependencies
- Functions called/Symbols referenced:
  - PGContextVisibility (enum type)
  - PQSHOW_CONTEXT_ERRORS (default context visibility constant)
- Called from (representative examples):
  - [SyncVariables](../S/SyncVariables.md) (in psql command.c)
  - [show_context_hook](../s/show_context_hook.md) (in psql startup.c)

## Notes and Other Information
- Returns the previous context visibility setting for restoration purposes
- If connection is NULL, returns PQSHOW_CONTEXT_ERRORS as a safe default
- Context information helps developers debug complex queries and stored procedures
- The setting affects all subsequent error messages from the connection
- Commonly used by interactive tools like psql to provide user-configurable error detail levels
- Context information is particularly useful for debugging PL/pgSQL functions and complex SQL statements

## Simplified Source

```c
PGContextVisibility PQsetErrorContextVisibility(PGconn *conn, PGContextVisibility show_context) {
    // Return default if no connection
    if (!conn)
        return PQSHOW_CONTEXT_ERRORS;

    // Save current context visibility setting
    PGContextVisibility old = conn->show_context;

    // Set new context visibility level
    conn->show_context = show_context;

    // Return previous setting
    return old;
}
```