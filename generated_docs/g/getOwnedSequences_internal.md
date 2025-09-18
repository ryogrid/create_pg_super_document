# getOwnedSequences_internal

## Location
src/backend/catalog/pg_depend.c: 878 - 936

## Overview
Collects a list of OIDs for all sequences owned by a specified table and optionally a specific column, with optional filtering by dependency type.

## Definition


## Detailed Description
The `getOwnedSequences_internal` function is a static utility that searches the `pg_depend` system catalog to find sequences that have ownership dependencies on a given table or specific column within that table. It serves as the core implementation for higher-level functions that need to identify owned sequences.

The function performs a systematic scan of the `pg_depend` table using the DependReferenceIndexId index for efficient lookups. It searches for dependency records where:
- The `refclassid` is RelationRelationId and `refobjid` matches the specified table
- If `attnum` is provided, `refobjsubid` must match the specified column number
- The `classid` is RelationRelationId (dependency originates from a relation)
- The `objsubid` is 0 (dependency is on the whole sequence, not a subcomponent)
- The `refobjsubid` is not 0 (dependency targets a specific column)
- The dependency type is either DEPENDENCY_AUTO or DEPENDENCY_INTERNAL
- The dependent object is confirmed to be a sequence via `get_rel_relkind`

If a `deptype` filter is specified, only sequences with that exact dependency type are included in the results. The function builds and returns a list containing the OIDs of all matching sequences.

## Parameters / Member Variables
- `relid`: The OID of the table whose owned sequences should be found
- `attnum`: The column number to search for (0 means search all columns of the table)
- `deptype`: Optional filter for dependency type (0 means include both AUTO and INTERNAL dependencies)

## Dependencies
- Functions called/Symbols referenced:
  - SysScanDesc
  - systable_beginscan
  - systable_getnext
  - Form_pg_depend
  - DEPENDENCY_AUTO
  - DEPENDENCY_INTERNAL
  - get_rel_relkind
  - RELKIND_SEQUENCE
  - lappend_oid
- Called from (representative examples):
  - getOwnedSequences
  - getIdentitySequence

## Notes and Other Information
- This is a static function, only accessible within the pg_depend.c file
- The function includes a relkind check to ensure dependent objects are actually sequences, since indexes can also have auto dependencies on columns
- Returns NIL (empty list) if no owned sequences are found
- Uses the DependReferenceIndexId index for efficient scanning by referenced object
- The distinction between AUTO and INTERNAL dependencies reflects different types of sequence ownership (e.g., SERIAL vs IDENTITY columns)
- This function is the foundation for sequence ownership tracking in PostgreSQL's dependency system