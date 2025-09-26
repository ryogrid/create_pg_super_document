# PQfformat

## Location
src/interfaces/libpq/fe-exec.c: 3708 - 3718

## Overview
Returns the format code indicating whether the specified field in a query result is in text or binary format.

## Definition
int PQfformat(const PGresult *res, int field_num)

## Detailed Description
PQfformat retrieves the format code for the specified field in a query result, indicating whether the field data is in text or binary format. This function is part of PostgreSQL's libpq client library and provides essential information for properly interpreting result data. The format code determines how the field data should be parsed: text format (0) provides human-readable string representations, while binary format (1) provides the raw binary representation as stored internally by PostgreSQL. The format for each field is determined by the query execution context and any explicit format specifications in the original query.

## Parameters / Member Variables
- res: Pointer to a PGresult structure containing the query result
- field_num: Zero-based index of the field (column) for which to retrieve the format code

## Dependencies
- Functions called/Symbols referenced:
  - check_field_number: Validates that field_num is within valid range
- Called from (representative examples):
  - process_queued_fetch_requests (pg_rewind): Verifies binary format for file data transfers
  - ecpg_get_data (ECPG): Determines how to process field data in embedded SQL
  - ecpg_store_result (ECPG): Handles format-specific result processing

## Notes and Other Information
- Returns 0 for text format, 1 for binary format
- Returns 0 if the field number is out of range or if attribute information is not available
- The function accesses the format member of the PGresAttDesc structure stored in res->attDescs
- Text format (0) provides NULL-terminated string representations of values
- Binary format (1) provides native PostgreSQL internal format, requiring type-specific parsing
- This function is thread-safe as it only reads from the PGresult structure
- Binary format is typically used for performance-critical applications and bulk data operations
- Defined in src/interfaces/libpq/fe-exec.c:3708-3718