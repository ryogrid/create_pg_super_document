# heap_toast_insert_or_update

## Location
src/backend/access/heap/heaptoast.c: 96 - 349

## Overview
Handles TOAST processing for INSERT or UPDATE operations by compressing and/or externalizing large attributes to make the tuple fit within size constraints.

## Definition
```c
HeapTuple heap_toast_insert_or_update(Relation rel, HeapTuple newtup, HeapTuple oldtup, int options)
```

## Detailed Description
The `heap_toast_insert_or_update` function implements PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) processing for new or updated tuples. It follows a multi-phase strategy to reduce tuple size:

1. **Phase 1**: Inline compress attributes with EXTENDED storage, and externalize very large EXTENDED/EXTERNAL attributes immediately
2. **Phase 2**: Externalize remaining EXTENDED/EXTERNAL attributes that are still inline  
3. **Phase 3**: Inline compress attributes with MAIN storage
4. **Phase 4**: Externalize MAIN attributes (with higher size threshold)

The function preserves the original input tuples and returns either the original tuple (if no toasting needed) or a new palloc'd tuple with modified values. It handles both INSERT (oldtup=NULL) and UPDATE scenarios, cleaning up old toast entries that are no longer referenced.

## Parameters / Member Variables
- `rel`: The relation being inserted into or updated
- `newtup`: The candidate new tuple to be inserted/updated  
- `oldtup`: The old row version for UPDATE operations, or NULL for INSERT
- `options`: Options to be passed to heap_insert() for toast rows

## Dependencies
- Functions called/Symbols referenced:
  - heap_deform_tuple
  - toast_tuple_init
  - toast_tuple_find_biggest_attribute  
  - toast_tuple_try_compression
  - toast_tuple_externalize
  - toast_tuple_cleanup
  - heap_compute_data_size
  - heap_fill_tuple
  - RelationGetToastTupleTarget
- Called from (representative examples):
  - heap_prepare_insert
  - heap_update
  - raw_heap_insert

## Notes and Other Information
- Only operates on plain relations (RELKIND_RELATION) and materialized views (RELKIND_MATVIEW)
- Uses a four-phase approach with progressively more aggressive strategies to reduce tuple size
- Implements different size thresholds for MAIN vs EXTENDED/EXTERNAL storage types
- Handles speculative insertions by filtering out HEAP_INSERT_SPECULATIVE option
- The algorithm is designed to avoid unnecessary work by externalizing very large values early
- Returns original tuple unchanged if no toasting is required, otherwise returns a new tuple