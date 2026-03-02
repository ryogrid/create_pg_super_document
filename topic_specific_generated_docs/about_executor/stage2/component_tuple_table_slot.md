# TupleTableSlot Abstraction

## Overview

The `TupleTableSlot` is the fundamental data carrier in the PostgreSQL executor.
Every tuple flowing through the executor -- whether read from disk, computed by
an expression, or received from a child plan node -- is stored in a
TupleTableSlot. The slot provides a uniform interface for accessing tuple data
while supporting multiple underlying storage formats through a virtual method
table (`TupleTableSlotOps`).

The design uses an object-oriented pattern implemented in C: a base
`TupleTableSlot` struct with a virtual method table pointer (`tts_ops`), and
four concrete implementations that extend the base with type-specific fields.

Source files:
- `src/include/executor/tuptable.h` (523 lines) -- type definitions, inline functions
- `src/backend/executor/execTuples.c` (2,426 lines) -- implementation

## Key Concepts

- **Lazy deforming**: Tuple data is extracted from physical tuples on demand.
  The `tts_nvalid` field tracks how many attributes have been deformed so far.
  This avoids unnecessary work for queries that only access a subset of columns.
- **Virtual tuples**: An optimization that avoids physical tuple construction.
  The Datum/isnull arrays directly hold computed values with no backing physical
  tuple.
- **Materialization**: The process of making a slot's contents independent of
  external resources (buffers, other memory contexts). Required before a tuple
  can be stored long-term.
- **Slot reuse**: Slots are allocated once during initialization and reused
  throughout execution. `ExecClearTuple()` releases the current contents
  without deallocating the slot.

## Architecture

```
See: diagrams/tuple_slot_hierarchy.mermaid
```

## Data Structures

### TupleTableSlot (Base)

```c
/* Source: src/include/executor/tuptable.h:114-131 */
typedef struct TupleTableSlot
{
    NodeTag     type;
    uint16      tts_flags;          /* Boolean states (see flag bits) */
    AttrNumber  tts_nvalid;         /* # of valid values in tts_values */
    const TupleTableSlotOps *const tts_ops; /* implementation of slot */
    TupleDesc   tts_tupleDescriptor; /* slot's tuple descriptor */
    Datum      *tts_values;         /* current per-attribute values */
    bool       *tts_isnull;         /* current per-attribute isnull flags */
    MemoryContext tts_mcxt;         /* slot itself is in this context */
    ItemPointerData tts_tid;        /* stored tuple's tid */
    Oid         tts_tableOid;       /* table oid of tuple */
} TupleTableSlot;
```

### Flag Bits

```c
/* Source: src/include/executor/tuptable.h:94-108 */
#define TTS_FLAG_EMPTY      (1 << 1)  /* slot holds no valid data */
#define TTS_FLAG_SHOULDFREE (1 << 2)  /* should pfree owned tuple? */
#define TTS_FLAG_SLOW       (1 << 3)  /* saved state for deform */
#define TTS_FLAG_FIXED      (1 << 4)  /* fixed tuple descriptor */
```

| Flag | Purpose |
|------|---------|
| `TTS_FLAG_EMPTY` | Slot contains no valid data. Set initially and after `ExecClearTuple()`. Must be cleared before accessing slot contents. |
| `TTS_FLAG_SHOULDFREE` | The slot "owns" the physical tuple and should pfree it when the tuple is replaced or the slot is cleared. |
| `TTS_FLAG_SLOW` | Internal state for `slot_deform_heap_tuple()`. Set when the previous deform stopped at a variable-length attribute, requiring offset recalculation. |
| `TTS_FLAG_FIXED` | The tuple descriptor is fixed for the slot's lifetime and cannot be changed by `ExecSetSlotDescriptor()`. |

---

### VirtualTupleTableSlot

```c
/* Source: src/include/executor/tuptable.h:244-251 */
typedef struct VirtualTupleTableSlot
{
    TupleTableSlot base;
    char       *data;       /* data for materialized slots */
} VirtualTupleTableSlot;
```

**Ops**: `TTSOpsVirtual`

A virtual slot contains only Datum/isnull arrays -- there is no physical tuple
backing the data. Pass-by-reference Datums point to storage elsewhere (typically
in a lower plan node's slot or in the per-tuple expression context).

**Usage**: Projection results, computed expressions, RETURNING results, VALUES
scan output.

**Characteristics**:
- No system columns (ctid, xmin, etc.) available
- `materialize` copies referenced data into `data` buffer
- Cheapest slot type for computed results

---

### HeapTupleTableSlot

```c
/* Source: src/include/executor/tuptable.h:253-264 */
typedef struct HeapTupleTableSlot
{
    TupleTableSlot base;
    HeapTuple   tuple;      /* physical tuple */
    uint32      off;        /* saved state for slot_deform_heap_tuple */
    HeapTupleData tupdata;  /* optional workspace for storing tuple */
} HeapTupleTableSlot;
```

**Ops**: `TTSOpsHeapTuple`

Holds a palloc'd physical HeapTuple. The slot can "own" the tuple (when
`TTS_FLAG_SHOULDFREE` is set), in which case it will be freed on clear.

**Usage**: Materialized tuples that need to outlive their source buffer, trigger
OLD/NEW tuple slots, SPI result tuples.

**Characteristics**:
- Has system columns
- `tuple` points to a palloc'd HeapTuple
- `off` tracks deform progress for lazy extraction

---

### MinimalTupleTableSlot

```c
/* Source: src/include/executor/tuptable.h:282-301 */
typedef struct MinimalTupleTableSlot
{
    TupleTableSlot base;
    HeapTuple   tuple;          /* tuple wrapper */
    MinimalTuple mintuple;      /* minimal tuple, or NULL */
    HeapTupleData minhdr;       /* workspace for minimal-tuple-only case */
    uint32      off;            /* saved state for slot_deform_heap_tuple */
} MinimalTupleTableSlot;
```

**Ops**: `TTSOpsMinimalTuple`

Holds a MinimalTuple, which is a compact representation that omits system columns
and the HeapTupleHeader fields not needed for in-memory processing. The `minhdr`
workspace and `tuple` pointer allow the deforming code to treat minimal tuples
identically to regular HeapTuples.

**Usage**: Hash join inner tuples, hash aggregate groups, tuplestore entries,
inter-process tuple queues (parallel query).

**Characteristics**:
- No system columns
- More compact than HeapTuple (saves about 23 bytes per tuple)
- `minhdr.t_data` points `MINIMAL_TUPLE_OFFSET` bytes before `mintuple`
- Optimized for in-memory operations

---

### BufferHeapTupleTableSlot

```c
/* Source: src/include/executor/tuptable.h:267-280 */
typedef struct BufferHeapTupleTableSlot
{
    HeapTupleTableSlot base;
    Buffer      buffer;     /* tuple's buffer, or InvalidBuffer */
} BufferHeapTupleTableSlot;
```

**Ops**: `TTSOpsBufferHeapTuple`

Extends `HeapTupleTableSlot` with a `buffer` field. The slot holds a pin on the
indicated buffer page, ensuring the physical tuple in shared buffers remains
valid. The pin is released when the slot is cleared.

**Usage**: Tuples returned by heap scans (SeqScan, IndexScan pointing to heap),
bitmap heap scan results.

**Characteristics**:
- Has system columns
- `TTS_FLAG_SHOULDFREE` is NOT set (tuple lives in shared buffer, not palloc'd)
- Pin on buffer is managed automatically
- Most common slot type for table scan results

---

## TupleTableSlotOps (Virtual Method Table)

```c
/* Source: src/include/executor/tuptable.h:134-223 */
struct TupleTableSlotOps
{
    size_t      base_slot_size;
    void        (*init) (TupleTableSlot *slot);
    void        (*release) (TupleTableSlot *slot);
    void        (*clear) (TupleTableSlot *slot);
    void        (*getsomeattrs) (TupleTableSlot *slot, int natts);
    Datum       (*getsysattr) (TupleTableSlot *slot, int attnum, bool *isnull);
    bool        (*is_current_xact_tuple) (TupleTableSlot *slot);
    void        (*materialize) (TupleTableSlot *slot);
    void        (*copyslot) (TupleTableSlot *dstslot, TupleTableSlot *srcslot);
    HeapTuple   (*get_heap_tuple) (TupleTableSlot *slot);
    MinimalTuple (*get_minimal_tuple) (TupleTableSlot *slot);
    HeapTuple   (*copy_heap_tuple) (TupleTableSlot *slot);
    MinimalTuple (*copy_minimal_tuple) (TupleTableSlot *slot);
};
```

### Method Summary

| Method | Purpose | Notes |
|--------|---------|-------|
| `init` | One-time initialization after slot creation | Slot-type-specific setup |
| `release` | Destruction of slot-specific resources | Called when slot is freed |
| `clear` | Release current tuple contents | Releases pin, pfrees if SHOULDFREE |
| `getsomeattrs` | Deform first N attributes from physical tuple | Updates tts_nvalid |
| `getsysattr` | Fetch a system attribute (ctid, xmin, etc.) | Errors for Virtual/Minimal |
| `is_current_xact_tuple` | Check if tuple was created by current xact | For snapshot optimization |
| `materialize` | Make slot contents independent of external storage | Copies buffer tuple to palloc'd |
| `copyslot` | Copy contents from source slot | Used by destination slot's ops |
| `get_heap_tuple` | Return owned HeapTuple (if available) | NULL for Virtual/Minimal |
| `get_minimal_tuple` | Return owned MinimalTuple (if available) | NULL for Virtual/Heap/Buffer |
| `copy_heap_tuple` | Return palloc'd copy as HeapTuple | Always available |
| `copy_minimal_tuple` | Return palloc'd copy as MinimalTuple | Always available |

### Predefined Instances

```c
/* Source: src/include/executor/tuptable.h:229-232 */
extern const TupleTableSlotOps TTSOpsVirtual;
extern const TupleTableSlotOps TTSOpsHeapTuple;
extern const TupleTableSlotOps TTSOpsMinimalTuple;
extern const TupleTableSlotOps TTSOpsBufferHeapTuple;
```

Type checking macros:
```c
#define TTS_IS_VIRTUAL(slot)      ((slot)->tts_ops == &TTSOpsVirtual)
#define TTS_IS_HEAPTUPLE(slot)    ((slot)->tts_ops == &TTSOpsHeapTuple)
#define TTS_IS_MINIMALTUPLE(slot) ((slot)->tts_ops == &TTSOpsMinimalTuple)
#define TTS_IS_BUFFERTUPLE(slot)  ((slot)->tts_ops == &TTSOpsBufferHeapTuple)
```

---

## Slot Lifecycle Operations

### Slot Creation

| Function | Purpose | Usage Context |
|----------|---------|---------------|
| `MakeTupleTableSlot(tupdesc, ops)` | Create standalone slot | General use |
| `ExecAllocTableSlot(tupleTable, desc, ops)` | Create slot registered with EState | Plan node init |
| `MakeSingleTupleTableSlot(tupdesc, ops)` | Create standalone slot (convenience) | Utility code |
| `ExecInitResultTupleSlotTL(ps, ops)` | Create result slot from plan target list | ExecInit* routines |
| `ExecInitScanTupleSlot(estate, ss, tupdesc, ops)` | Create scan slot | Scan node init |
| `ExecInitExtraTupleSlot(estate, tupdesc, ops)` | Create extra slot | Junk filter, etc. |

### Storing Tuples

| Function | Source Type | Slot Type Required | Pin/Free Handling |
|----------|------------|-------------------|-------------------|
| `ExecStoreHeapTuple(tuple, slot, shouldFree)` | HeapTuple | HeapTupleTableSlot | Sets SHOULDFREE if requested |
| `ExecStoreBufferHeapTuple(tuple, slot, buffer)` | Buffer-resident | BufferHeapTupleTableSlot | Holds pin on buffer |
| `ExecStorePinnedBufferHeapTuple(tuple, slot, buffer)` | Buffer (caller pins) | BufferHeapTupleTableSlot | Caller retains pin |
| `ExecStoreMinimalTuple(mtup, slot, shouldFree)` | MinimalTuple | MinimalTupleTableSlot | Sets SHOULDFREE if requested |
| `ExecStoreVirtualTuple(slot)` | Datum/isnull arrays | Any (typically Virtual) | Marks slot non-empty |
| `ExecStoreAllNullTuple(slot)` | None | Any | Fills with NULLs |
| `ExecForceStoreHeapTuple(tuple, slot, shouldFree)` | HeapTuple | Any (converts if needed) | Forces storage |
| `ExecForceStoreMinimalTuple(mtup, slot, shouldFree)` | MinimalTuple | Any (converts if needed) | Forces storage |

### Clearing and Materializing

```c
/* Source: src/include/executor/tuptable.h:453-475 */
static inline TupleTableSlot *
ExecClearTuple(TupleTableSlot *slot)
{
    slot->tts_ops->clear(slot);
    return slot;
}

static inline void
ExecMaterializeSlot(TupleTableSlot *slot)
{
    slot->tts_ops->materialize(slot);
}
```

`ExecClearTuple` releases the current tuple (freeing it if SHOULDFREE, releasing
buffer pin if BufferHeap), sets TTS_FLAG_EMPTY, and resets tts_nvalid to 0.

`ExecMaterializeSlot` makes the slot's contents independent of external
resources. For BufferHeapTupleTableSlot, this copies the tuple out of the shared
buffer into palloc'd memory and releases the buffer pin.

### Copying

```c
/* Source: src/include/executor/tuptable.h:508-519 */
static inline TupleTableSlot *
ExecCopySlot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
    Assert(!TTS_EMPTY(srcslot));
    Assert(srcslot != dstslot);
    dstslot->tts_ops->copyslot(dstslot, srcslot);
    return dstslot;
}
```

| Function | Returns | Allocates In |
|----------|---------|-------------|
| `ExecCopySlot(dst, src)` | Destination slot | Destination slot's context |
| `ExecCopySlotHeapTuple(slot)` | Palloc'd HeapTuple | Current memory context |
| `ExecCopySlotMinimalTuple(slot)` | Palloc'd MinimalTuple | Current memory context |

---

## Deforming: Lazy Attribute Extraction

The executor uses lazy deforming to avoid extracting all columns from a physical
tuple when only some are needed. This is a significant optimization for wide
tables.

### Core Functions

```c
/* Source: src/include/executor/tuptable.h:354-359 */
static inline void
slot_getsomeattrs(TupleTableSlot *slot, int attnum)
{
    if (slot->tts_nvalid < attnum)
        slot_getsomeattrs_int(slot, attnum);
}
```

The inline check of `tts_nvalid` avoids the function call overhead when
attributes are already deformed. The actual deforming work happens in
`slot_getsomeattrs_int()` (defined in `execTuples.c`), which calls the
slot's `getsomeattrs` virtual method.

```c
/* Source: src/include/executor/tuptable.h:394-406 */
static inline Datum
slot_getattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
    Assert(attnum > 0);
    if (attnum > slot->tts_nvalid)
        slot_getsomeattrs(slot, attnum);
    *isnull = slot->tts_isnull[attnum - 1];
    return slot->tts_values[attnum - 1];
}
```

### Integration with Expression Evaluation

The expression compiler generates `EEOP_INNER_FETCHSOME`, `EEOP_OUTER_FETCHSOME`,
and `EEOP_SCAN_FETCHSOME` steps that call `slot_getsomeattrs()` at the start of
expression evaluation. The `natts` parameter is the maximum attribute number
referenced in the expression, ensuring all needed attributes are deformed before
any `EEOP_*_VAR` step accesses them.

---

## Slot Type Selection by Node Type

| Plan Node Category | Typical Slot Type | Reason |
|-------------------|-------------------|--------|
| SeqScan, IndexScan, BitmapHeapScan | BufferHeapTuple | Tuples reside in shared buffers |
| Hash inner side | MinimalTuple | Compact storage in hash table |
| Sort, Material | MinimalTuple | Stored in tuplesort/tuplestore |
| Projection (Result, most nodes) | Virtual | Computed Datum/isnull arrays |
| NestLoop, MergeJoin output | Virtual | Projected join result |
| ModifyTable RETURNING | Virtual or HeapTuple | Depends on operation |
| FunctionScan | HeapTuple or Virtual | Function-dependent |
| Parallel tuple queue | MinimalTuple | Serialized through shared memory |

---

## Buffer Manager Interaction

For `BufferHeapTupleTableSlot`, the slot maintains a pin on the buffer page
containing the tuple. The lifecycle is:

1. **Store**: `ExecStoreBufferHeapTuple(tuple, slot, buffer)` stores the tuple
   pointer and increments the buffer pin count via `IncrBufferRefCount()`.
2. **Access**: While the slot holds the tuple, the buffer pin guarantees the
   page will not be evicted from shared buffers.
3. **Clear**: `ExecClearTuple(slot)` calls `ReleaseBuffer(buffer)` to drop the pin.
4. **Materialize**: `ExecMaterializeSlot(slot)` copies the tuple to palloc'd
   memory and releases the buffer pin, making the tuple independent of the buffer.

This is critical for correctness: without the pin, the buffer pool could evict
the page, leaving the slot pointing to invalid memory.

## Implementation Notes

- The `tts_ops` field is declared `const` to prevent accidental modification at
  runtime. The slot type is fixed at creation.
- The `FIELDNO_*` defines (e.g., `FIELDNO_TUPLETABLESLOT_VALUES`) allow JIT-
  compiled code to access struct fields by known offsets without depending on
  the C compiler's layout decisions.
- Virtual slots are the cheapest to create and use because they avoid all
  physical tuple overhead. The expression evaluation system produces virtual
  tuples by directly writing into the slot's Datum/isnull arrays.
- `TupIsNull(slot)` is the canonical way to test for end-of-scan:
  ```c
  #define TupIsNull(slot) ((slot) == NULL || TTS_EMPTY(slot))
  ```
  It handles both NULL slot pointers and empty slots.
