# SPI_connect

## Location
[src/backend/executor/spi.c:94-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L94-L99)

## Overview
SPI_connect establishes a connection to PostgreSQL's Server Programming Interface (SPI), enabling stored procedures and functions to execute SQL commands within the server process.

## Definition

```c
int
SPI_connect(void)
```
## Detailed Description
SPI_connect is a simple wrapper function that calls SPI_connect_ext(0) with default options. It initializes the SPI connection stack, creates memory contexts for procedure execution, and sets up the necessary internal state for SQL command execution from within server-side code. The function provides backward compatibility while the extended version (SPI_connect_ext) allows for additional connection options.

When called, this function:
- Enlarges the SPI connection stack if necessary (initially to 16 entries, doubles when full)
- Enters a new stack level by incrementing _SPI_connected
- Initializes a new _SPI_connection structure with default atomic behavior
- Creates memory contexts (procCxt and execCxt) for the procedure
- Resets global SPI variables (SPI_processed, SPI_tuptable, SPI_result)
- Returns SPI_OK_CONNECT on success

## Parameters / Member Variables
This function takes no parameters and uses default connection options (atomic mode enabled).

## Dependencies
- Functions called/Symbols referenced:
  - SPI_connect_ext
  
- Called from (representative examples):
  - refresh_by_match_merge (materialized view operations)
  - ri_Check_Pk_Match (referential integrity checks)
  - ri_restrict, RI_FKey_cascade_del, RI_FKey_cascade_upd (foreign key constraint handling)
  - pg_get_ruledef_worker, pg_get_viewdef_worker (rule and view definition functions)
  - plperl_trigger_handler, pltcl_trigger_handler (procedural language trigger handlers)
  - Various XML processing functions (cursor_to_xml, query_to_xml_internal, etc.)

## Notes and Other Information
- Must be paired with SPI_finish() to properly clean up the connection
- Creates an atomic execution context by default (transactions are managed automatically)
- Part of PostgreSQL's SPI subsystem that allows server-side code to execute SQL
- The SPI connection is stored on a stack, allowing for nested SPI connections
- Memory contexts created during connection are automatically cleaned up during transaction end or explicit SPI_finish()
- Located in src/backend/executor/spi.c:94-99