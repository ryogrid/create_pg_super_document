# PQsetErrorContextVisibility

## Location
src/interfaces/libpq/fe-connect.c: 7309 - 7320

## Overview
Controls the visibility of context information in error messages, allowing applications to specify when context details should be included in error reports.

## Definition


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
  - SyncVariables (in psql command.c)
  - show_context_hook (in psql startup.c)

## Notes and Other Information
- Returns the previous context visibility setting for restoration purposes
- If connection is NULL, returns PQSHOW_CONTEXT_ERRORS as a safe default
- Context information helps developers debug complex queries and stored procedures
- The setting affects all subsequent error messages from the connection
- Commonly used by interactive tools like psql to provide user-configurable error detail levels
- Context information is particularly useful for debugging PL/pgSQL functions and complex SQL statements