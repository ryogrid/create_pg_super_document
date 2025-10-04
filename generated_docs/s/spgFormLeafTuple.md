# spgFormLeafTuple

## Location
[src/backend/access/spgist/spgutils.c:863-951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L863-L951)

## Overview
Constructs a complete SP-GiST leaf tuple containing a heap TID reference and attribute data values, with proper memory layout and null value handling.

## Definition

```c
SpGistLeafTuple
spgFormLeafTuple(SpGistState *state, ItemPointer heapPtr,
				 const Datum *datums, const bool *isnulls)
```
## Detailed Description
This function creates a properly formatted SP-GiST leaf tuple that stores a reference to a heap tuple along with the indexed attribute values. The function implements the same size calculation logic as SpGistGetLeafTupleSize and then constructs the actual tuple structure.

Key aspects of the tuple formation process:

1. **Null bitmap handling**: Uses the same compatibility logic as SpGistGetLeafTupleSize - single-attribute tuples never use null bitmasks for pre-v14 compatibility, while multi-attribute tuples include a null bitmap only when needed.

2. **Memory allocation**: Allocates zero-initialized memory using palloc0() to ensure clean tuple state.

3. **Header initialization**: Sets up the tuple header including size, next offset (initially invalid), and heap pointer.

4. **Data filling**: Uses heap_fill_tuple() to populate the tuple data area following heap tuple conventions, with conditional null bitmap handling.

5. **Minimum size enforcement**: Ensures the tuple meets minimum size requirements for future dead tuple replacement.

## Parameters / Member Variables
- `*state`: SpGistState structure containing index configuration and type descriptors
- `heapPtr`: ItemPointer referencing the corresponding heap tuple
- `*datums`: Array of Datum values for each indexed attribute
- `*isnulls`: Array of boolean flags indicating which attributes are null
## Dependencies
- Functions called/Symbols referenced:
  - [SpGistState](../S/SpGistState.md) (index state structure)
  - [SpGistLeafTuple](../S/SpGistLeafTuple.md) (return type structure)
  - [heap_compute_data_size](../h/heap_compute_data_size.md) (data size calculation)
  - SGLTHDRSZ (header size macro)
  - SGDTSIZE (dead tuple size constant)
  - SGLT_SET_NEXTOFFSET (next offset setter macro)
  - SGLT_SET_HASNULLMASK (null mask flag setter)
  - [heap_fill_tuple](../h/heap_fill_tuple.md) (data population function)
  - spgKeyColumn (key column identifier)
- Called from (representative examples):
  - [doPickSplit](../d/doPickSplit.md) (during node splitting operations)
  - [spgdoinsert](spgdoinsert.md) (during index insertion)

## Notes and Other Information
- The function must stay synchronized with SpGistGetLeafTupleSize for consistent size calculations
- Uses heap tuple data layout conventions, making leaf tuples similar to regular heap tuples in structure
- The compatibility logic ensures backward compatibility with PostgreSQL versions before v14
- Memory is zero-initialized to avoid uninitialized data in padding areas
- The tuple can later be replaced with a dead tuple marker due to minimum size enforcement

## Simplified Source

```c
SpGistLeafTuple spgFormLeafTuple(SpGistState *state, ItemPointer heapPtr,
                                const Datum *datums, const bool *isnulls) {
    TupleDesc tupleDescriptor = state->leafTupDesc;
    Size size, hoff, data_size;
    bool needs_null_mask = false;
    int natts = tupleDescriptor->natts;

    // Determine null mask requirement (same logic as SpGistGetLeafTupleSize)
    if (natts > 1) {
        for (int i = 0; i < natts; i++) {
            if (isnulls[i]) {
                needs_null_mask = true;
                break;
            }
        }
    }

    // Calculate sizes
    data_size = heap_compute_data_size(tupleDescriptor, datums, isnulls);
    hoff = SGLTHDRSZ(needs_null_mask);
    size = hoff + data_size;
    size = MAXALIGN(size);

    // Ensure minimum size for dead tuple replacement
    if (size < SGDTSIZE)
        size = SGDTSIZE;

    // Allocate and initialize tuple
    SpGistLeafTuple tup = (SpGistLeafTuple) palloc0(size);
    tup->size = size;
    SGLT_SET_NEXTOFFSET(tup, InvalidOffsetNumber);
    tup->heapPtr = *heapPtr;

    // Fill tuple data
    char *tp = (char *) tup + hoff;

    if (needs_null_mask) {
        // Set null mask flag and fill data with null bitmap
        SGLT_SET_HASNULLMASK(tup, true);
        bits8 *bp = (bits8 *) ((char *) tup + sizeof(SpGistLeafTupleData));
        uint16 tupmask = 0;
        heap_fill_tuple(tupleDescriptor, datums, isnulls, tp, data_size,
                       &tupmask, bp);
    } else if (natts > 1 || !isnulls[spgKeyColumn]) {
        // Fill data area without null bitmap
        uint16 tupmask = 0;
        heap_fill_tuple(tupleDescriptor, datums, isnulls, tp, data_size,
                       &tupmask, (bits8 *) NULL);
    }
    // If single null attribute, no data to fill

    return tup;
}
```