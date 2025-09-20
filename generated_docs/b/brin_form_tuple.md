# brin_form_tuple

## Location
[src/backend/access/brin/brin_tuple.c:99-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_tuple.c#L99-L387)

## Overview
Generates a new on-disk tuple to be inserted in a BRIN index, converting in-memory BRIN summary data into a serialized format suitable for storage.

## Definition

```c
struct varlena *)
															  DatumGetPointer(value)));
```
## Detailed Description
This function transforms a BRIN memory tuple into a disk-storable format. It handles complex operations including value serialization, TOAST decompression/compression, null bitmap management, and memory layout optimization. The function processes each column's summary values, handles special cases like all-null columns and null-containing ranges, and creates a properly formatted on-disk tuple with correct alignment and null bitmap encoding.

Key operations include:
- Converting memory tuple values to disk format using type-specific serialization
- Managing TOAST values by detoasting external values and attempting compression
- Creating dual null bitmaps for "allnulls" and "hasnulls" states
- Computing proper memory layout with alignment requirements
- Setting appropriate tuple flags for placeholder and empty range states

## Parameters / Member Variables
- : BRIN descriptor containing schema information, type details, and cached structures
- : Block number this tuple represents in the BRIN index structure
- : In-memory BRIN tuple containing the summary data to be serialized
- : Output parameter to receive the total size of the created on-disk tuple

## Dependencies
- Functions called/Symbols referenced:
  - [BrinDesc](../B/BrinDesc.md), BrinMemTuple, BrinTuple (structure types)
  - [brtuple_disk_tupdesc](brtuple_disk_tupdesc.md)
  - [heap_compute_data_size](../h/heap_compute_data_size.md)
  - [heap_fill_tuple](../h/heap_fill_tuple.md)
  - TOAST handling functions (detoast_external_attr, toast_compress_datum)
  - Memory management functions (palloc, palloc0, pfree)
- Called from:
  - [brininsert](brininsert.md) (src/backend/access/brin/brin.c:464)
  - [summarize_range](../s/summarize_range.md) (src/backend/access/brin/brin.c:1830)
  - [form_and_insert_tuple](../f/form_and_insert_tuple.md) (src/backend/access/brin/brin.c:1981)
  - [form_and_spill_tuple](../f/form_and_spill_tuple.md) (src/backend/access/brin/brin.c:2006)
  - [_brin_parallel_merge](_brin_parallel_merge.md) (src/backend/access/brin/brin.c:2698, 2730)
  - [brin_build_empty_tuple](brin_build_empty_tuple.md) (src/backend/access/brin/brin.c:2954)
  - BrinTupleIsEmptyRange (src/include/access/brin_tuple.h:96)

## Notes and Other Information
- Handles variable-length and TOAST values through conditional compilation with TOAST_INDEX_HACK
- Uses a unique null bitmap encoding where 1 represents null (opposite of standard PostgreSQL convention)
- Implements dual null bitmaps: first half for "allnulls" bits, second half for "hasnulls" bits
- Performs memory management carefully to avoid leaks when detoasting/compressing values
- Critical for BRIN index maintenance operations and tuple insertion workflows
- The function must coordinate with brin_form_placeholder_tuple for consistency