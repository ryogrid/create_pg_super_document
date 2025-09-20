# StoreIndexTuple

## Location
[src/backend/executor/nodeIndexonlyscan.c:268-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L268-L324)

## Overview
Fills a TupleTableSlot with data extracted from an index tuple, handling datatype conversions and special cases for name columns stored as C strings.

## Definition

```c
static void
StoreIndexTuple(IndexOnlyScanState *node, TupleTableSlot *slot,
				IndexTuple itup, TupleDesc itupdesc)
```
## Detailed Description
StoreIndexTuple is a specialized function that converts data from an IndexTuple into a TupleTableSlot format suitable for query execution. The function deforms the index tuple into separate Datum values and null indicators, then stores them in the provided slot.

A key aspect of this function is handling the special case of name columns that are stored as C strings in the index but need to be converted to fixed-length NAMEDATALEN-sized allocations. This conversion is necessary for compatibility with PostgreSQL's name datatype, which is commonly used in system catalogs.

The function uses the tuple descriptor provided by the access method rather than the slot's descriptor to ensure proper datatype handling, particularly important for cases like btree name_ops where datatypes may differ between index and table representations.

## Parameters / Member Variables
- : IndexOnlyScanState containing scan state and configuration information including name column attributes
- : TupleTableSlot to be filled with the index tuple data  
- : IndexTuple containing the source data to be stored
- : TupleDesc from the access method describing the index tuple structure

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple: Clears the tuple slot before storing new data
  - [index_deform_tuple](../i/index_deform_tuple.md): Converts IndexTuple into separate Datum arrays
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocates memory for name column conversions
  - namestrcpy: Copies C string to fixed-length name with zero-padding
  - [DatumGetCString](../D/DatumGetCString.md): Extracts C string from Datum value
  - [NameGetDatum](../N/NameGetDatum.md): Converts Name to Datum for storage
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md): Finalizes the virtual tuple in the slot
- Called from (representative examples):
  - [IndexOnlyNext](../I/IndexOnlyNext.md): Uses this function to store index tuple data when heap tuple is not available

## Notes and Other Information
- The function specifically handles datatype compatibility issues between index and table representations
- Special optimization for name columns stored as C strings, converting them to proper NAMEDATALEN-sized allocations
- Memory allocation for name conversions uses the per-tuple expression context for automatic cleanup
- The name column conversion is marked as unlikely since it primarily affects system catalog queries
- Asserts that the slot and index tuple descriptors have the same number of attributes for safety