# ECPGprepare

## Location
src/interfaces/ecpg/ecpglib/prepare.c: 217 - 238

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
  - ecpg_get_connection
  - ecpg_init
  - ecpg_find_prepared_statement
  - deallocate_one
  - prepare_common
- Structures used:
  - connection
  - prepared_statement
- Constants used:
  - ECPG_COMPAT_PGSQL
- Called from (representative examples):
  - ecpg_auto_prepare
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