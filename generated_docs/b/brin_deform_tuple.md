# brin_deform_tuple

## Location
[src/backend/access/brin/brin_tuple.c:553-644](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_tuple.c#L553-L644)

## Overview
Converts a serialized BrinTuple from disk format back to an in-memory BrinMemTuple representation, performing the reverse operation of brin_form_tuple.

## Definition
BrinMemTuple *brin_deform_tuple(BrinDesc *brdesc, BrinTuple *tuple, BrinMemTuple *dMemtuple)

## Detailed Description
This function deserializes a BrinTuple from its on-disk storage format into a BrinMemTuple suitable for in-memory manipulation. It handles the reconstruction of column values, null flags, and metadata from the compact disk representation. The function can either allocate a new BrinMemTuple or reuse a provided one for optimization. It processes null bitmaps, extracts data values using brin_deconstruct_tuple, and copies each datum value into the appropriate column structure while preserving type information and null states.

## Parameters / Member Variables
- brdesc: Pointer to BrinDesc structure containing tuple descriptor and type information needed for deserialization
- tuple: Pointer to the serialized BrinTuple to convert from disk format
- dMemtuple: Optional pointer to pre-allocated BrinMemTuple to reuse (can be NULL to allocate new)

## Dependencies
- Functions called/Symbols referenced:
  - [brin_memtuple_initialize](brin_memtuple_initialize.md) (initializes memory tuple structure)
  - [brin_new_memtuple](brin_new_memtuple.md) (allocates new memory tuple if needed)
  - BrinTupleIsPlaceholder (checks if tuple is a placeholder)
  - BrinTupleIsEmptyRange (checks if tuple represents empty range)
  - BrinTupleHasNulls (checks for null values in tuple)
  - BrinTupleDataOffset (calculates data offset in tuple)
  - SizeOfBrinTuple (gets base tuple size)
  - [brin_deconstruct_tuple](brin_deconstruct_tuple.md) (extracts values from disk format)
  - [datumCopy](../d/datumCopy.md) (copies datum values)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (manages memory contexts)
- Called from (representative examples):
  - [brininsert](brininsert.md)
  - [bringetbitmap](bringetbitmap.md)  
  - [union_tuples](../u/union_tuples.md)
  - brin_parallel_merge
  - BrinTupleIsEmptyRange

## Notes and Other Information
- Supports optimization by reusing pre-allocated BrinMemTuple structures to avoid repeated allocations
- Properly handles placeholder tuples and empty range indicators from disk format
- Copies all datum values using appropriate type information for by-value vs by-reference types
- Uses the tuple's memory context for storing copied datum values
- Sets up column metadata including serialization pointers and context references
- Does not require the on-disk tuple descriptor as it uses internal deconstruction routines

## Simplified Source

```c
BrinMemTuple *brin_deform_tuple(BrinDesc *brdesc, BrinTuple *tuple, BrinMemTuple *dMemtuple) {
    BrinMemTuple *dtup;
    Datum *values;
    bool *allnulls;
    bool *hasnulls;
    char *tp;
    bits8 *nullbits;
    int keyno, valueno;
    MemoryContext oldcxt;

    // Initialize memory tuple (reuse provided or create new)
    dtup = dMemtuple ? brin_memtuple_initialize(dMemtuple, brdesc) :
           brin_new_memtuple(brdesc);

    // Handle special tuple types
    if (BrinTupleIsPlaceholder(tuple))
        dtup->bt_placeholder = true;
    if (!BrinTupleIsEmptyRange(tuple))
        dtup->bt_empty_range = false;

    // Set block number
    dtup->bt_blkno = tuple->bt_blkno;

    // Setup working arrays
    values = dtup->bt_values;
    allnulls = dtup->bt_allnulls;
    hasnulls = dtup->bt_hasnulls;

    // Extract tuple data and null bits
    tp = (char *) tuple + BrinTupleDataOffset(tuple);
    nullbits = BrinTupleHasNulls(tuple) ?
               (bits8 *) ((char *) tuple + SizeOfBrinTuple) : NULL;

    // Deconstruct the raw tuple data
    brin_deconstruct_tuple(brdesc, tp, nullbits, BrinTupleHasNulls(tuple),
                          values, allnulls, hasnulls);

    // Copy values to tuple's memory context
    oldcxt = MemoryContextSwitchTo(dtup->bt_context);
    for (valueno = 0, keyno = 0; keyno < brdesc->bd_tupdesc->natts; keyno++) {
        if (allnulls[keyno]) {
            valueno += brdesc->bd_info[keyno]->oi_nstored;
            continue;
        }

        // Copy each stored value for this column
        for (int i = 0; i < brdesc->bd_info[keyno]->oi_nstored; i++) {
            dtup->bt_columns[keyno].bv_values[i] =
                datumCopy(values[valueno++],
                         brdesc->bd_info[keyno]->oi_typcache[i]->typbyval,
                         brdesc->bd_info[keyno]->oi_typcache[i]->typlen);
        }

        // Set column metadata
        dtup->bt_columns[keyno].bv_hasnulls = hasnulls[keyno];
        dtup->bt_columns[keyno].bv_allnulls = false;
        dtup->bt_columns[keyno].bv_mem_value = PointerGetDatum(NULL);
        dtup->bt_columns[keyno].bv_serialize = NULL;
        dtup->bt_columns[keyno].bv_context = dtup->bt_context;
    }

    MemoryContextSwitchTo(oldcxt);
    return dtup;
}
```