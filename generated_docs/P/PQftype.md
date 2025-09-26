# PQftype

## Location
src/interfaces/libpq/fe-exec.c: 3719 - 3729

## Overview
Returns the PostgreSQL type OID for the specified field in a query result.

## Definition
Oid PQftype(const PGresult *res, int field_num)

## Detailed Description
PQftype retrieves the PostgreSQL type OID (Object Identifier) for the specified field in a query result. This function is part of PostgreSQL's libpq client library and provides essential type information for properly interpreting and processing result data. The type OID uniquely identifies the PostgreSQL data type of the field, such as INTEGER, VARCHAR, TIMESTAMP, etc. This information is crucial for applications that need to perform type-specific processing, data conversion, or validation. The function accesses the type information that is populated by the server during query execution.

## Parameters / Member Variables
- res: Pointer to a PGresult structure containing the query result
- field_num: Zero-based index of the field (column) for which to retrieve the type OID

## Dependencies
- Functions called/Symbols referenced:
  - check_field_number: Validates that field_num is within valid range
  - InvalidOid: Constant representing an invalid OID value
- Called from (representative examples):
  - dumpTableData_insert (pg_dump): Type checking for data export
  - process_queued_fetch_requests (pg_rewind): Type validation for file transfers
  - DescribeQuery (psql): Type information display
  - printCrosstab (psql): Type-aware formatting for crosstab output
  - printQuery (psql): General type-aware result formatting
  - ECPG functions: Type handling in embedded SQL
  - libpq_pipeline tests: Type verification in testing

## Notes and Other Information
- Returns InvalidOid (0) if the field number is out of range or if attribute information is not available
- The function accesses the typid member of the PGresAttDesc structure stored in res->attDescs
- Type OIDs are consistent across PostgreSQL instances and can be used for reliable type identification
- Common type OIDs include standard PostgreSQL types like INT4OID, TEXTOID, TIMESTAMPOID, etc.
- This function is thread-safe as it only reads from the PGresult structure
- Type information is essential for binary format processing and proper data marshaling
- The type OID can be used with PostgreSQL system catalogs to get detailed type information
- Defined in src/interfaces/libpq/fe-exec.c:3719-3729