# HeapTupleData

## Location
[src/include/access/htup.h:62-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/htup.h#L62-L69)

## Overview
HeapTupleData is PostgreSQL's primary in-memory data structure that serves as a pointer and metadata container for tuples, supporting various tuple representations including disk buffer tuples, palloc'd tuples, and minimal tuples.

## Definition


## Detailed Description
HeapTupleData is the fundamental in-memory tuple representation in PostgreSQL, acting as a versatile pointer structure that can reference tuples in multiple formats and storage locations. Unlike HeapTupleHeaderData which is the actual tuple header stored on disk, HeapTupleData serves as a lightweight wrapper providing access to tuple data regardless of its storage format.

The structure supports five distinct usage patterns: pointing to tuples in disk buffers, indicating failure with NULL t_data, representing palloc'd tuples where the data follows immediately after the structure, separately allocated tuples, and minimal tuples with offset positioning. This flexibility allows the same access routines to work across all tuple representations.

The design enables efficient tuple handling throughout PostgreSQL's execution pipeline, from storage layer access to executor operations, providing a consistent interface while accommodating different memory layouts and storage optimizations.

## Parameters / Member Variables
- : Length in bytes of the tuple data pointed to by t_data, always valid except when t_data is NULL
- : ItemPointerData containing the tuple's disk location (TID), valid for disk tuples or copies of disk tuples  
- : Object identifier of the table this tuple originates from, valid for disk tuples and their copies
- : Pointer to HeapTupleHeader containing the actual tuple header and data, can point to various memory layouts

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](../I/ItemPointerData.md)
  - HeapTupleHeader
  - Oid
- Called from (representative examples):
  - [heap_get_latest_tid](../h/heap_get_latest_tid.md) (src/backend/access/heap/heapam.c:1859)
  - [heap_delete](../h/heap_delete.md) (src/backend/access/heap/heapam.c:2738)
  - [heap_update](../h/heap_update.md) (src/backend/access/heap/heapam.c:3214)
  - [ExecMakeTableFunctionResult](../E/ExecMakeTableFunctionResult.md) (src/backend/executor/execSRF.c:115)
  - [GetAttributeByName](../G/GetAttributeByName.md) (src/backend/executor/execUtils.c:1002)

## Notes and Other Information
- The typedef HeapTuple creates a pointer type for convenient usage throughout the codebase
- HEAPTUPLESIZE macro defines the aligned size for allocating HeapTupleData structures
- For palloc'd tuples, t_data points at offset HEAPTUPLESIZE after the HeapTupleData struct
- For minimal tuples, t_data points MINIMAL_TUPLE_OFFSET bytes before the MinimalTuple start
- The structure cannot distinguish between disk buffer pointers and separately allocated tuples by inspection alone
- Code must explicitly set t_self and t_tableOid to invalid values for manufactured (non-disk) tuples
- Serves as the primary interface for all tuple access routines in PostgreSQL's executor and storage layers
- Critical component enabling PostgreSQL's flexible tuple handling across different storage formats and memory layouts