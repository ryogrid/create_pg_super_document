# ecpg_process_output

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1671-1943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1671-L1943)

## Overview
Processes SQL statement results and transfers data into application variables, handling various result types and output formats.

## Definition

```c
bool
ecpg_process_output(struct statement *stmt, bool clear_result)
```
## Detailed Description
This is a comprehensive result processing function that handles the complex task of transferring query results from PostgreSQL into application variables. It supports multiple output scenarios:

- **PGRES_TUPLES_OK**: Processes SELECT results into regular variables, SQL descriptors, or SQLDA structures
- **PGRES_COMMAND_OK**: Handles INSERT/UPDATE/DELETE commands, updating SQLCA with row counts and OIDs  
- **PGRES_COPY_OUT**: Manages COPY TO STDOUT operations by streaming data to stdout

The function intelligently handles different variable types (regular variables, descriptors, SQLDA), manages memory allocation/deallocation for complex structures, performs data type conversions, validates field counts, and provides comprehensive error reporting. It also processes asynchronous notifications and maintains SQLCA state information.

## Parameters / Member Variables
- : Pointer to statement structure containing the PGresult to process and the list of output variables to populate
- : Boolean flag indicating whether to call PQclear() on the result when finished (supports result reuse scenarios like cursor operations)

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca: Gets SQLCA structure for error/status reporting
  - PQresultStatus: Determines result type
  - PQnfields/PQntuples: Gets result dimensions  
  - PQcmdStatus/PQoidValue/PQcmdTuples: Gets command status info
  - ecpg_find_desc: Locates SQL descriptors
  - ecpg_build_compat_sqlda/ecpg_build_native_sqlda: Builds SQLDA structures
  - ecpg_set_compat_sqlda/ecpg_set_native_sqlda: Populates SQLDA with data
  - ecpg_store_result: Transfers data to regular variables
  - PQgetCopyData: Handles COPY operations
  - PQconsumeInput/PQnotifies: Processes asynchronous notifications
  - ecpg_raise: Reports errors
  - ecpg_log: Provides debugging output
- Called from (representative examples):
  - ecpg_do: Main ECPG statement processing function

## Notes and Other Information
- Returns true on successful processing, false on failure
- Supports cursor readahead scenarios where function may be called repeatedly
- Handles both Informix-compatible and native PostgreSQL SQLDA formats
- Automatically manages memory for complex data structures
- Processes asynchronous notifications after main result processing
- Critical component in ECPG's data transfer pipeline between PostgreSQL and embedded applications
- Validates argument counts and raises appropriate errors for mismatches