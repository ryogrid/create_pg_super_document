# CatalogIndexState

## Location
src/include/catalog/indexing.h: 27 - 32

## Overview
CatalogIndexState is a type alias for ResultRelInfo that provides an abstraction layer for managing system catalog index operations, decoupling callers from the executor's internal ResultRelInfo structure.

## Definition


## Detailed Description
CatalogIndexState serves as a state object used by catalog indexing functions like CatalogOpenIndexes and related operations. Despite being functionally identical to the executor's ResultRelInfo structure, it is given a distinct type name to provide architectural separation and cleaner interfaces for catalog management operations.

This typedef creates an abstraction that allows catalog indexing code to operate independently of the executor's internal data structures, making the codebase more modular and maintainable. The underlying ResultRelInfo structure contains comprehensive information about result relations, including index descriptors, trigger information, and various metadata needed for catalog operations.

The abstraction is particularly useful for catalog operations that need to:
- Maintain indexes on system catalogs during tuple insertion/update/deletion
- Handle multi-insert operations with proper index maintenance
- Manage catalog-specific constraints and triggers
- Provide a clean interface for catalog indexing without exposing executor internals

## Parameters / Member Variables
Since CatalogIndexState is a pointer to ResultRelInfo, it inherits all the member variables from that structure:

- : Range table index (0 if not in range table)
- : Relation descriptor for the result relation
- : Number of indices existing on the result relation
- : Array of relation descriptors for indices
- : Array of key/attribute info for indices
- : Triggers to be fired, if any
- : Projection to generate new tuple in INSERT/UPDATE
- : Slot to hold new tuples
- : Slot to hold old tuples being updated
- Additional fields for batch operations, constraints, and partition handling

## Dependencies
- Functions using CatalogIndexState:
  - CatalogCloseIndexes
  - CatalogIndexInsert
  - CatalogTupleInsert
  - CatalogTupleInsertWithInfo
  - CatalogTuplesMultiInsertWithInfo
  - CatalogTupleUpdate
  - CatalogTupleUpdateWithInfo
  - InsertPgAttributeTuples
  - AddNewAttributeTuples
  - AppendAttributeTuples
- Called from (representative examples):
  - heap.c catalog operations
  - index.c catalog maintenance
  - pg_depend.c dependency tracking
  - analyze.c statistics updates
  - cluster.c relation file operations

## Notes and Other Information
- The abstraction allows catalog code to remain independent of executor implementation changes
- Used extensively throughout the catalog system for maintaining consistency of system catalogs
- The MAX_CATALOG_MULTI_INSERT_BYTES constant (defined in the same header) works in conjunction with CatalogIndexState to limit memory usage during multi-insert operations
- Despite being a simple typedef, it represents an important architectural decision to separate concerns between the executor and catalog subsystems
- Performance-critical path for all catalog modifications in PostgreSQL