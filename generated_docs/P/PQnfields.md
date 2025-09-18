# PQnfields

## Location
src/interfaces/libpq/fe-exec.c: 3489 - 3496

## Overview
PQnfields returns the number of columns (fields) in a query result set stored in a PGresult object.

## Definition
int PQnfields(const PGresult *res)

## Detailed Description
This function provides access to the count of columns returned by a SQL query. It is a fundamental function for examining the structure of query results in the libpq interface. The function returns the value stored in the numAttributes field of the PGresult structure, which contains the number of attributes (columns) in the result set. This information is essential for applications that need to process query results dynamically without prior knowledge of the result structure.

The function handles NULL input gracefully by returning 0, making it safe to use without explicit NULL checking in many contexts.

## Parameters / Member Variables
- : Pointer to the PGresult object containing the query results

## Dependencies
- Functions called/Symbols referenced:
  - None (accesses res->numAttributes directly)
- Called from (representative examples):
  - libpqrcv_identify_system (replication system identification)
  - BaseBackup (pg_basebackup result processing)
  - dumpTableData_insert (pg_dump data processing)
  - StoreQueryTuple (psql result storage)
  - PrintResultInCrosstab (psql crosstab display)
  - ECPGget_desc_header (ECPG descriptor handling)
  - ecpg_process_output (ECPG result processing)
  - PQprint (libpq result printing)

## Notes and Other Information
- Returns 0 if the PGresult pointer is NULL
- Returns the exact number of columns in the result set
- Commonly used in conjunction with PQntuples to determine result set dimensions
- Essential for iterating through query result columns
- Used to validate expected result structure in applications
- Part of the core libpq result inspection API
- Critical for dynamic result processing where column count is unknown at compile time
- Used extensively throughout PostgreSQL client tools and applications
- Often called before column iteration loops to determine bounds