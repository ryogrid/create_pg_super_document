# recordDependencyOnTablespace

## Location
src/backend/catalog/pg_shdepend.c: 370 - 390

## Overview
A convenience wrapper function that records a tablespace dependency relationship between a database object and its default tablespace.

## Definition


## Detailed Description
This function simplifies the process of recording tablespace dependencies in PostgreSQL. It constructs ObjectAddress structures for both the dependent object and the tablespace, then calls recordSharedDependencyOn with SHARED_DEPENDENCY_TABLESPACE type to establish the tablespace relationship. The tablespace is always referenced from the pg_tablespace catalog (TableSpaceRelationId). This dependency tracking is essential for preventing tablespaces from being dropped while objects still depend on them.

## Parameters / Member Variables
- : OID of the system catalog that contains the dependent object  
- : OID of the dependent object within its catalog
- : OID of the tablespace from pg_tablespace catalog

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddressSet
  - recordSharedDependencyOn
  - SHARED_DEPENDENCY_TABLESPACE (dependency type constant)
- Called from (representative examples):
  - heap_create
  - (Limited usage - primarily during table creation with explicit tablespace)

## Notes and Other Information
- It's the caller's responsibility to ensure no tablespace entry already exists for the object
- Uses ObjectAddressSet macro for clean ObjectAddress initialization
- The tablespace is always referenced through TableSpaceRelationId (pg_tablespace catalog)
- This function helps enforce referential integrity for tablespace dependencies
- Part of PostgreSQL's shared dependency tracking system for cross-database objects
- Located in src/backend/catalog/pg_shdepend.c:370-390