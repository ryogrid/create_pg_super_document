# heapam_index_validate_scan

## Location
[src/backend/access/heap/heapam_handler.c:1748-1994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L1748-L1994)

## Overview
Validates an index by scanning the heap relation and comparing it against existing index entries, inserting any missing tuples that should be indexed.

## Definition
```c
static void heapam_index_validate_scan(Relation heapRelation,
                                     Relation indexRelation,
                                     IndexInfo *indexInfo,
                                     Snapshot snapshot,
                                     ValidateIndexState *state)
```

## Detailed Description
This function performs index validation by conducting a merge-like scan between heap tuples and existing index entries. It scans the heap relation using the provided snapshot and compares each tuple against sorted index entries from a tuplesort to identify missing index entries. When a heap tuple is found that should be indexed but is missing from the index, it inserts the tuple into the index.

The function handles HOT (Heap-Only-Tuples) chains by converting actual tuple TIDs to root TIDs using heap_get_root_tuples mapping. It maintains an in_index array to track which tuples on the current page have already been processed from the tuplesort. The validation process ensures that concurrent index builds or interrupted index builds can be completed by adding any missing entries.

## Parameters / Member Variables
- `heapRelation`: The heap table being validated against the index
- `indexRelation`: The index being validated and potentially updated
- `indexInfo`: Index metadata including uniqueness constraints and predicates
- `snapshot`: Snapshot defining which tuple versions to consider during validation
- `state`: Validation state tracking progress and statistics (htups, tups_inserted)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_getnext](heap_getnext.md)
  - [heap_get_root_tuples](heap_get_root_tuples.md)
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md)
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - [index_insert](../i/index_insert.md)
  - [ExecQual](../E/ExecQual.md)
  - [ItemPointerCompare](../I/ItemPointerCompare.md)
  - [table_beginscan_strat](../t/table_beginscan_strat.md)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)

## Notes and Other Information
This function is critical for concurrent index builds and index repair operations. It must handle the complexity of merging heap scan results with sorted index entries while properly managing HOT chains. The function disables synchronized scanning to ensure TIDs are read in the correct order for comparison. The validation process accounts for partial indexes by evaluating predicates and handles uniqueness checking appropriately, even for tuples that might be dead but part of HOT chains.