# index_create

## Location
[src/backend/catalog/index.c:724-1297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L724-L1297)

## Overview
index_create is the primary function for creating a new index relation in PostgreSQL, handling all aspects from relation creation to catalog entries and dependency management.

## Definition

```c
enumber map if and only if the parent does;
```
## Detailed Description
This function performs the complete process of creating a new index relation. It validates parameters, creates the physical relation structure, registers catalog entries in pg_class, pg_index, and pg_attribute, handles inheritance relationships for partitioned indexes, creates constraints if requested, records all necessary dependencies, and optionally builds the index data.

The function supports various index creation modes including concurrent creation, partitioned indexes, constraint creation, and can handle both regular and system table modifications. It performs extensive validation including checking for duplicate names, validating collation compatibility with operator classes, and ensuring system catalog restrictions are observed.

## Parameters / Member Variables
- : The table relation to build the index on (must be suitably locked)
- : Name for the new index relation
- : OID for the index (InvalidOid to auto-generate)
- : OID of parent index for partitioned indexes (InvalidOid otherwise)
- : OID of parent constraint for partitioned constraints (InvalidOid otherwise)
- : File number for index storage (InvalidRelFileNumber for new storage)
- : IndexInfo structure containing index metadata and properties
- : List of column names for the index
- : OID of the index access method to use
- : OID of tablespace where index should be created
- : Array of collation OIDs for index key columns
- : Array of operator class OIDs for index key columns
- : Array of opclass-specific options for index columns
- : Array of per-column index options
- : Array of statistics targets for index columns
- : Access method specific relation options
- : Bitmask controlling creation behavior (primary key, concurrent, etc.)
- : Additional flags for constraint creation
- : Whether to allow creating indexes on system tables
- : Whether this is an internal index creation
- : Output parameter receiving OID of created constraint

## Dependencies
- Functions called/Symbols referenced:
  - [heap_create](../h/heap_create.md) (for creating the index relation)
  - [ConstructTupleDescriptor](../C/ConstructTupleDescriptor.md) (for building index tuple descriptor)
  - [UpdateIndexRelation](../U/UpdateIndexRelation.md) (for pg_index catalog entry)
  - [InsertPgClassTuple](../I/InsertPgClassTuple.md) (for pg_class catalog entry)
  - [InitializeAttributeOids](../I/InitializeAttributeOids.md) (for attribute OID assignment)
  - [AppendAttributeTuples](../A/AppendAttributeTuples.md) (for pg_attribute entries)
  - [index_constraint_create](index_constraint_create.md) (for constraint creation)
  - [StoreSingleInheritance](../S/StoreSingleInheritance.md) (for partitioned index inheritance)
  - [recordDependencyOn](../r/recordDependencyOn.md) (for dependency recording)
  - [index_build](index_build.md) (for building index data)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (from CREATE INDEX command)
  - [create_toast_table](../c/create_toast_table.md) (for TOAST table indexes)
  - [index_concurrently_create_copy](index_concurrently_create_copy.md) (for concurrent index creation)

## Notes and Other Information
- Returns the OID of the created index relation
- The function handles both regular and partitioned index creation
- Supports concurrent index creation with special validation and marking
- Performs extensive parameter validation and error checking
- Creates all necessary catalog entries and dependency relationships
- Can skip index building phase for deferred construction (ALTER TABLE scenarios)
- Properly handles inheritance relationships for partitioned tables
- Located at src/backend/catalog/index.c:724-1297