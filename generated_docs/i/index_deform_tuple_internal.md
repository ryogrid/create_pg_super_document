# index_deform_tuple_internal

## Location
[src/backend/access/common/indextuple.c:479-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/indextuple.c#L479-L546)

## Overview
The `index_deform_tuple_internal` function converts an index tuple into separate Datum/isnull arrays without making assumptions about the index tuple header layout, providing flexible tuple deformation capabilities.

## Definition
```c
void index_deform_tuple_internal(TupleDesc tupleDescriptor, Datum *values, bool *isnull, char *tp, bits8 *bp, int hasnulls)
```

## Detailed Description
This function performs the core logic for deforming IndexTuples into their constituent attribute values and null indicators. Unlike the wrapper `index_deform_tuple` function, this internal version accepts explicit pointers to the tuple data area and null bitmap, making it more flexible for different tuple layouts.

The function implements sophisticated offset caching logic similar to `nocache_index_getattr`:
- Uses cached offsets when available and valid
- Handles alignment requirements for both fixed-width and variable-length attributes
- Manages the transition between "fast" (cacheable) and "slow" (non-cacheable) processing modes
- Processes attributes sequentially, extracting values and updating null indicators

Key optimizations include:
- Caching attribute offsets in the tuple descriptor for future reuse
- Efficient null handling that skips null attributes without affecting alignment calculations
- Proper alignment handling for variable-length attributes with conditional caching
- Early termination of caching when encountering nulls or variable-length attributes

## Parameters
- `tupleDescriptor`: TupleDesc describing the expected structure and attributes of the tuple
- `values`: Output array to store extracted Datum values
- `isnull`: Output array to store null indicators for each attribute
- `tp`: Pointer to the tuple data area (excluding header)
- `bp`: Pointer to the null bitmap (can be NULL if !hasnulls)
- `hasnulls`: Boolean flag indicating whether the tuple contains any null values

## Dependencies
- Functions called/Symbols referenced:
  - [att_isnull](../a/att_isnull.md)
  - att_align_nominal
  - att_align_pointer
  - att_addlength_pointer
  - fetchatt
- Constants used:
  - INDEX_MAX_KEYS (for validation)
- Data types used:
  - bits8 (for null bitmap handling)
- Called from:
  - [index_deform_tuple](index_deform_tuple.md) (src/backend/access/common/indextuple.c:467)
  - [spgDeformLeafTuple](../s/spgDeformLeafTuple.md) (src/backend/access/spgist/spgutils.c:1135)

## Notes and Other Information
- Located in src/backend/access/common/indextuple.c:479-546
- Includes an assertion to protect callers who allocate fixed-size arrays (natts <= INDEX_MAX_KEYS)
- The function maintains a "slow" flag that tracks whether attribute offset caching can continue to be used
- [Variable](../V/Variable.md)-length attributes can only have their offsets cached if they are naturally aligned, avoiding padding issues
- Once a null attribute or problematic alignment is encountered, the function switches to "slow" mode and stops caching offsets
- Used by both regular index tuple deformation and specialized access methods like SP-GiST
- The flexible interface allows it to work with different tuple header layouts and external null bitmap arrangements

## Simplified Source

```c
void index_deform_tuple_internal(TupleDesc tupleDescriptor, Datum *values, bool *isnull,
                                char *tp, bits8 *bp, int hasnulls) {
    int natts = tupleDescriptor->natts;
    int attnum;
    int off = 0;          // Current offset in tuple data
    bool slow = false;    // Whether we can use cached offsets

    Assert(natts <= INDEX_MAX_KEYS);

    // Process each attribute
    for (attnum = 0; attnum < natts; attnum++) {
        Form_pg_attribute thisatt = TupleDescAttr(tupleDescriptor, attnum);

        // Handle null values
        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum) 0;
            isnull[attnum] = true;
            slow = true;  // Can't cache offsets anymore
            continue;
        }

        isnull[attnum] = false;

        // Use cached offset if available and we're still in fast mode
        if (!slow && thisatt->attcacheoff >= 0) {
            off = thisatt->attcacheoff;
        }
        // Handle variable-length attributes with special alignment
        else if (thisatt->attlen == -1) {
            if (!slow && off == att_align_nominal(off, thisatt->attalign))
                thisatt->attcacheoff = off;
            else {
                off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
                slow = true;
            }
        }
        // Handle fixed-length attributes
        else {
            off = att_align_nominal(off, thisatt->attalign);
            if (!slow)
                thisatt->attcacheoff = off;
        }

        // Extract the attribute value
        values[attnum] = fetchatt(thisatt, tp + off);

        // Move to next attribute position
        off = att_addlength_pointer(off, thisatt->attlen, tp + off);

        // Variable-length attributes disable caching
        if (thisatt->attlen <= 0)
            slow = true;
    }
}
```