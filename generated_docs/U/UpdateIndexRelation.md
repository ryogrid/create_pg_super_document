# UpdateIndexRelation

## Location
src/backend/catalog/index.c: 561 - 723

## Overview
UpdateIndexRelation is a static function that constructs and inserts a new entry in the pg_index system catalog to record metadata about an index relation.

## Definition

```c
static void
UpdateIndexRelation(Oid indexoid,
					Oid heapoid,
					Oid parentIndexId,
					const IndexInfo *indexInfo,
					const Oid *collationOids,
					const Oid *opclassOids,
					const int16 *coloptions,
					bool primary,
					bool isexclusion,
					bool immediate,
					bool isvalid,
					bool isready)
```
## Detailed Description
This function creates a complete entry in the pg_index system catalog with all necessary metadata for an index. It processes the provided index information and transforms it into the appropriate format for storage in the catalog. The function handles conversion of index expressions and predicates to text format, builds various vector types for storing index keys and options, and performs the actual catalog insertion.

The function is responsible for setting all index flags and properties including uniqueness, primary key status, exclusion constraints, validity, and readiness states. It serves as the central point for recording index metadata during index creation operations.

## Parameters / Member Variables
- : Object identifier of the index relation being created
- : Object identifier of the table (heap) that this index belongs to
- : Object identifier of parent index (for partitioned indexes)
- : IndexInfo structure containing index attribute numbers, expressions, predicates, and other properties
- : Array of collation object identifiers for each index key column
- : Array of operator class object identifiers for each index key column
- : Array of option flags for each index key column
- : Boolean flag indicating if this is a primary key index
- : Boolean flag indicating if this is an exclusion constraint index
- : Boolean flag indicating if constraint checking is immediate
- : Boolean flag indicating if the index is valid for queries
- : Boolean flag indicating if the index is ready for inserts

## Dependencies
- Functions called/Symbols referenced:
  - buildint2vector (for index keys and options)
  - buildoidvector (for collations and operator classes)
  - nodeToString (for expressions and predicates)
  - make_ands_explicit (for predicate normalization)
  - heap_form_tuple (for tuple construction)
  - CatalogTupleInsert (for catalog insertion)
  - heap_freetuple (for memory cleanup)
- Called from (representative examples):
  - index_create

## Notes and Other Information
- This is a static function internal to index.c, not exposed in the public API
- The function properly handles NULL values for optional fields like expressions and predicates
- All boolean flags are set to appropriate defaults (e.g., indisclustered=false, indislive=true)
- The function uses RowExclusiveLock when accessing the pg_index catalog
- Memory management is handled properly with pfree() calls for temporary strings
- The function is located at src/backend/catalog/index.c:561-723