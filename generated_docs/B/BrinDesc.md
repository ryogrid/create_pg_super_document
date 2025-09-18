# BrinDesc

## Location
[src/include/access/brin_internal.h:44-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/brin_internal.h#L44-L63)

## Overview
BrinDesc is a comprehensive descriptor structure that enables decoding BRIN tuples from their on-disk format to in-memory representation and vice-versa, serving as the central metadata hub for BRIN index operations.

## Definition
```c
typedef struct BrinDesc
{
    /* Containing memory context */
    MemoryContext bd_context;

    /* the index relation itself */
    Relation    bd_index;

    /* tuple descriptor of the index relation */
    TupleDesc   bd_tupdesc;

    /* cached copy for on-disk tuples; generated at first use */
    TupleDesc   bd_disktdesc;

    /* total number of Datum entries that are stored on-disk for all columns */
    int         bd_totalstored;

    /* per-column info; bd_tupdesc->natts entries long */
    BrinOpcInfo *bd_info[FLEXIBLE_ARRAY_MEMBER];
} BrinDesc;
```

## Detailed Description
BrinDesc serves as the master descriptor for BRIN index operations, providing all necessary metadata to interpret and manipulate BRIN tuples. This structure bridges the gap between the logical index definition and the physical storage format, enabling efficient conversion between on-disk and in-memory tuple representations.

The structure maintains both the original tuple descriptor (bd_tupdesc) for the logical view and a cached disk tuple descriptor (bd_disktdesc) optimized for physical storage operations. This dual-descriptor approach allows PostgreSQL to handle differences between logical column definitions and their optimized physical storage representations.

The flexible array of BrinOpcInfo pointers (bd_info) provides per-column operator class information, enabling the system to correctly interpret and process different types of BRIN indexes (min/max, inclusion, bloom filter, etc.) within a single unified framework.

## Parameters / Member Variables
- `bd_context`: Memory context that contains this BrinDesc and related structures for proper memory management
- `bd_index`: Relation pointer to the BRIN index relation itself
- `bd_tupdesc`: Tuple descriptor representing the logical structure of the index
- `bd_disktdesc`: Cached tuple descriptor optimized for on-disk storage format, created on first use
- `bd_totalstored`: Total count of Datum entries stored on disk across all indexed columns
- `bd_info`: Flexible array of BrinOpcInfo pointers, one per indexed column, providing operator class-specific metadata

## Dependencies
- Functions called/Symbols referenced:
  - [BrinOpcInfo](BrinOpcInfo.md)
  - FLEXIBLE_ARRAY_MEMBER
  - [MemoryContext](../M/MemoryContext.md)
  - [Relation](../R/Relation.md)
  - [TupleDesc](../T/TupleDesc.md)

- Called from (representative examples):
  - [brininsert](../b/brininsert.md) (src/backend/access/brin/brin.c:346)
  - [bringetbitmap](../b/bringetbitmap.md) (src/backend/access/brin/brin.c:562)
  - [brin_build_desc](../b/brin_build_desc.md) (src/backend/access/brin/brin.c:1575, 1607)
  - [brin_free_desc](../b/brin_free_desc.md) (src/backend/access/brin/brin.c:1627)
  - [union_tuples](../u/union_tuples.md) (src/backend/access/brin/brin.c:2022)
  - [add_values_to_range](../a/add_values_to_range.md) (src/backend/access/brin/brin.c:2196)
  - [brin_form_tuple](../b/brin_form_tuple.md) (src/backend/access/brin/brin_tuple.c:99)
  - [brin_deform_tuple](../b/brin_deform_tuple.md) (src/backend/access/brin/brin_tuple.c:553)

## Notes and Other Information
- [BrinDesc](BrinDesc.md) is typically allocated in a long-lived memory context to persist across multiple index operations
- The bd_disktdesc is lazily initialized on first use to optimize memory usage
- This structure is essential for all BRIN tuple manipulation functions, providing the context needed for proper serialization/deserialization
- The bd_totalstored field helps optimize memory allocation and validation during tuple processing
- [BrinDesc](BrinDesc.md) instances are usually created by brin_build_desc() and should be freed using brin_free_desc()
- The structure supports heterogeneous column types within a single BRIN index through the per-column BrinOpcInfo array
- Memory management is crucial as BrinDesc holds references to potentially large structures like Relations and TupleDescs