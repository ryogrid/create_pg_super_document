# recordMultipleDependencies

## Location
src/backend/catalog/pg_depend.c: 58 - 193

## Overview
Records multiple dependencies of the same type for a single dependent object efficiently, using batch insertion to minimize overhead compared to recording each dependency separately.

## Definition


## Detailed Description
This function provides an optimized way to record multiple dependency relationships for a single dependent object. It creates entries in the pg_depend system catalog table using batch insertion techniques to improve performance when dealing with multiple dependencies. The function handles several optimizations including skipping pinned objects (which don't need dependency tracking), using tuple slots for efficient insertion, and batching insertions to reduce I/O overhead. It also handles bootstrap mode by returning early since pg_depend may not exist during system initialization.

## Parameters / Member Variables
- : Pointer to ObjectAddress of the dependent object (the one that depends on others)
- : Pointer to array of ObjectAddress structures representing the referenced objects
- : Integer count of how many referenced objects are in the array
- : DependencyType enum value specifying the type of dependency relationship for all entries

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - table_open
  - isObjectPinned
  - MakeSingleTupleTableSlot
  - ExecClearTuple
  - ExecStoreVirtualTuple
  - CatalogOpenIndexes
  - CatalogTuplesMultiInsertWithInfo
  - CatalogCloseIndexes
  - ExecDropSingleTupleTableSlot
  - MAX_CATALOG_MULTI_INSERT_BYTES
  - DependencyType
  - CatalogIndexState
  - CharGetDatum
- Called from (representative examples):
  - recordDependencyOnExpr
  - recordDependencyOnSingleRelExpr
  - record_object_address_dependencies
  - recordDependencyOn

## Notes and Other Information
- Located in src/backend/catalog/pg_depend.c:58-193
- Uses batch insertion with configurable slot buffer size based on MAX_CATALOG_MULTI_INSERT_BYTES
- Automatically skips pinned objects to save space in pg_depend catalog
- Returns early during bootstrap processing mode since pg_depend may not exist
- Does not check for duplicate dependencies - allows them without harm
- Opens indexes lazily only when actually needed for insertion
- Uses efficient tuple slot management with proper cleanup
- Optimized for cases where multiple objects depend on the same set of referenced objects