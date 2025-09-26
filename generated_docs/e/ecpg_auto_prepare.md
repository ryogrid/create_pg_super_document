# ecpg_auto_prepare

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:553-602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L553-L602)

## Overview
Handles cache and preparation of SQL statements in auto-prepare mode for ECPG, providing automatic statement caching and preparation to optimize repeated SQL execution.

## Definition
bool ecpg_auto_prepare(int lineno, const char *connection_name, const int compat, char **name, const char *query)

## Detailed Description
The  function implements an automatic statement preparation and caching mechanism for ECPG (Embedded C for PostgreSQL). This function first searches for the given SQL query in the statement cache. If found, it retrieves the cached statement ID and ensures the statement is prepared on the specified connection. If not found, it generates a new unique statement ID, prepares the statement using ECPGprepare, and adds it to the cache for future use. The function also tracks usage statistics by incrementing an execution counter for cached statements.

This mechanism optimizes performance by avoiding redundant preparation of identical SQL statements and provides transparent statement management in auto-prepare mode.

## Parameters / Member Variables
- : Line number in the source code where this function is called, used for error reporting and logging
- : Name of the database connection to use for statement preparation
- : Compatibility mode setting for ECPG processing
- : Output parameter that receives the allocated statement ID string for the prepared statement
- : The SQL query string to be prepared and cached

## Dependencies
- Functions called/Symbols referenced:
  - [SearchStmtCache](../S/SearchStmtCache.md)
  - [ecpg_log](ecpg_log.md)
  - [ecpg_get_connection](ecpg_get_connection.md)
  - [ecpg_find_prepared_statement](ecpg_find_prepared_statement.md)
  - [prepare_common](../p/prepare_common.md)
  - [ecpg_strdup](ecpg_strdup.md)
  - [ECPGprepare](../E/ECPGprepare.md)
  - [AddStmtToCache](../A/AddStmtToCache.md)
- Called from (representative examples):
  - [ecpg_do_prologue](ecpg_do_prologue.md)

## Notes and Other Information
- The function maintains a global statement cache () and statement ID counter ()
- Statement IDs are generated with the format "ecpg%d" using an incrementing counter
- The function tracks statement usage through an execution counter for performance monitoring
- Returns  on success,  on failure (preparation or caching errors)
- Part of the ECPG library's automatic statement management system, providing transparent optimization for embedded SQL applications