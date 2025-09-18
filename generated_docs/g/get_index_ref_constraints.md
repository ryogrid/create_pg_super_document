# get_index_ref_constraints

## Location
src/backend/catalog/pg_depend.c: 1045 - 1092

## Overview
Retrieves a list of OIDs for all foreign key constraints that reference a given index, used to identify dependencies that must be considered when modifying or dropping indexes.

## Definition
List *get_index_ref_constraints(Oid indexId)

## Detailed Description
This function searches the PostgreSQL dependency system to find all foreign key constraints that have normal dependencies on a specific index. It scans the pg_depend system catalog using the reference-based index to efficiently locate constraints that depend on the given index. The function is particularly important for foreign key constraint management, as foreign keys often reference unique or primary key indexes on the referenced table. The returned list contains the OIDs of all such foreign key constraints.

## Parameters / Member Variables
- `indexId`: The OID of the index for which to find referencing foreign key constraints

## Dependencies
- Functions called/Symbols referenced:
  - SysScanDesc
  - systable_beginscan
  - systable_getnext
  - Form_pg_depend
  - DEPENDENCY_NORMAL
  - lappend_oid
- Called from (representative examples):
  - index_concurrently_swap

## Notes and Other Information
The function uses the DependReferenceIndexId for efficient scanning of the pg_depend catalog, searching specifically for normal dependencies (as opposed to internal dependencies). This is crucial for foreign key constraint management since foreign keys establish normal dependency relationships with the indexes they reference. The function accumulates all matching constraint OIDs into a list, which allows callers to understand the full scope of foreign key dependencies before performing operations that might affect the index. This is essential for maintaining referential integrity in PostgreSQL's constraint system.