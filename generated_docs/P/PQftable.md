# PQftable

## Location
src/interfaces/libpq/fe-exec.c: 3686 - 3696

## Overview
Returns the OID of the table that is the source of the given field in a query result.

## Definition
Oid PQftable(const PGresult *res, int field_num)

## Detailed Description
PQftable retrieves the OID (Object Identifier) of the table that contains the specified field in a query result. This function is part of PostgreSQL's libpq client library and provides metadata about result columns. The table OID is only available when the query result includes information about the source tables of the columns, which typically occurs with SELECT statements that reference specific table columns. If the field does not originate from a table (e.g., computed expressions, constants) or if the source table information is not available, the function returns InvalidOid.

## Parameters / Member Variables
- res: Pointer to a PGresult structure containing the query result
- field_num: Zero-based index of the field (column) for which to retrieve the table OID

## Dependencies
- Functions called/Symbols referenced:
  - check_field_number: Validates that field_num is within valid range
  - InvalidOid: Constant representing an invalid OID value
- Called from (representative examples):
  - Client applications querying metadata about result columns
  - Tools that need to identify source tables of query results

## Notes and Other Information
- Returns InvalidOid (0) if the field number is out of range or if no table source information is available
- The function accesses the tableid member of the PGresAttDesc structure stored in res->attDescs
- Table OID information is populated by the server when available and depends on the nature of the SQL query
- This function is thread-safe as it only reads from the PGresult structure
- Defined in src/interfaces/libpq/fe-exec.c:3686-3696