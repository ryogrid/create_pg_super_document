# heapam_tuple_tid_valid

## Location
[src/backend/access/heap/heapam_handler.c:205-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L205-L213)

## Overview
This function validates whether a given tuple identifier (TID) is valid within the context of a heap table scan, checking both the TID's format validity and whether it references a block within the scan's range.

## Definition
```c
static bool
heapam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
```

## Detailed Description
heapam_tuple_tid_valid is a static callback function used by the heap access method to determine if a tuple identifier is valid for a given table scan. The function performs two key validations: first, it checks if the ItemPointer itself is valid (not null and properly formatted), and second, it verifies that the block number referenced by the TID is within the range of blocks being scanned (less than rs_nblocks in the heap scan descriptor).

This function is essential for ensuring that tuple access operations don't attempt to read from invalid or out-of-range locations, providing a safety check before actual tuple retrieval operations.

## Parameters / Member Variables
- `scan`: TableScanDesc containing the scan context, cast internally to HeapScanDesc to access heap-specific scan information
- `tid`: ItemPointer containing the tuple identifier to validate (block number and offset pair)

## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDesc](../T/TableScanDesc.md) (parameter type)
  - [HeapScanDesc](../H/HeapScanDesc.md) (type cast)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md) (TID format validation)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md) (block number extraction)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)

## Notes and Other Information
- This is a static function serving as a callback in the table access method interface
- The function assumes the scan parameter can be safely cast to HeapScanDesc
- Validation includes both format checking (ItemPointerIsValid) and range checking (block number < rs_nblocks)
- Returns true only if both the TID is valid and the referenced block is within the scan range
- Part of the heap access method's tuple validation infrastructure
- The rs_nblocks field represents the total number of blocks in the relation being scanned