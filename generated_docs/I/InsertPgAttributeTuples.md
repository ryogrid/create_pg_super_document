# InsertPgAttributeTuples

## Location
src/backend/catalog/heap.c: 703 - 820

## Overview
Constructs and inserts a set of tuples in the pg_attribute system catalog, efficiently batch-inserting multiple attribute definitions for a relation.

## Definition


## Detailed Description
InsertPgAttributeTuples is a low-level catalog management function that creates and inserts pg_attribute tuples for a set of attributes defined in a TupleDesc. The function is optimized for batch operations, creating multiple slots and inserting tuples in batches to improve performance when creating relations with many attributes.

The function copies attribute metadata from the provided TupleDesc into pg_attribute format, handling all the necessary data type conversions and field mappings. It supports both basic attribute information and extended attribute data through the tupdesc_extra parameter. The attcacheoff field is always initialized to -1, and several variable-length fields are set to null for new attributes.

The function uses a sophisticated batching mechanism that limits the number of slots based on memory constraints (MAX_CATALOG_MULTI_INSERT_BYTES) and processes attributes in groups to optimize catalog insertion performance.

## Parameters / Member Variables
- : An already opened and locked relation handle for the pg_attribute catalog
- : TupleDesc containing the attributes to insert into pg_attribute
- : Relation OID to assign to the inserted attributes; if InvalidOid, uses the relation OID from tupdesc
- : Optional array providing values for variable-length/nullable pg_attribute fields; must match tupdesc length or be NULL
- : Index state for CatalogTupleInsertWithInfo; can be NULL (will fetch necessary info automatically)

## Dependencies
- Functions called/Symbols referenced:
  - MakeSingleTupleTableSlot
  - ExecClearTuple
  - ExecStoreVirtualTuple
  - ExecDropSingleTupleTableSlot
  - CatalogOpenIndexes
  - CatalogTuplesMultiInsertWithInfo
  - CatalogCloseIndexes
  - Various Datum conversion functions (NameGetDatum, Int16GetDatum, etc.)
- Called from (representative examples):
  - AddNewAttributeTuples
  - AppendAttributeTuples
  - ATExecAddColumn

## Notes and Other Information
- The function is optimized for bulk insertion operations and should be preferred over single-tuple insertion for multiple attributes
- When inserting multiple attributes, it's more efficient to pass a valid indstate parameter rather than letting the function fetch index information repeatedly
- The function automatically handles memory management for the tuple slots and ensures proper cleanup
- Variable-length pg_attribute fields (attacl, attfdwoptions, attmissingval) are always set to null for new columns