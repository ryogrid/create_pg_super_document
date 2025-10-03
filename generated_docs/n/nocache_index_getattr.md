# nocache_index_getattr

## Location
[src/backend/access/common/indextuple.c:241-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/indextuple.c#L241-L455)

## Overview
The `nocache_index_getattr` function extracts a specific attribute value from an IndexTuple when cached offsets are not available, implementing an optimized attribute offset caching strategy.

## Definition
```c
Datum nocache_index_getattr(IndexTuple tup, int attnum, TupleDesc tupleDesc)
```

## Detailed Description
This function is called from the `index_getattr()` macro in cases where cached offsets cannot be used and the requested attribute value is not null. It implements a sophisticated attribute offset caching mechanism to optimize future attribute access operations.

The function handles three main scenarios:
1. **Fast path**: No nulls and no variable-width attributes up to the target attribute
2. **Null handling**: Presence of null values requiring careful navigation through the null bitmap
3. **Variable-width handling**: Variable-length attributes requiring dynamic offset calculation

Key optimizations include:
- Caching attribute offsets in the tuple descriptor for future use
- Bulk calculation of offsets for all leading fixed-width columns
- Careful null bitmap navigation to skip null attributes
- Alignment handling for both fixed-width and variable-length attributes

The caching strategy is designed to perform well for queries that access large numbers of tuples using the same attribute descriptor, as offset calculations are cached and reused across tuples.

## Parameters
- `tup`: IndexTuple from which to extract the attribute value
- `attnum`: 1-based attribute number to extract (gets decremented internally to 0-based)
- `tupleDesc`: TupleDesc describing the tuple structure and providing caching storage

## Dependencies
- Functions called/Symbols referenced:
  - [IndexInfoFindDataOffset](../I/IndexInfoFindDataOffset.md)
  - IndexTupleHasNulls
  - IndexTupleHasVarwidths
  - fetchatt
  - [att_isnull](../a/att_isnull.md)
  - att_align_nominal
  - att_align_pointer
  - att_addlength_pointer
- Data types used:
  - bits8 (for null bitmap navigation)
  - [IndexTupleData](../I/IndexTupleData.md)
- Called from:
  - [index_getattr](../i/index_getattr.md) macro (src/include/access/itup.h:134, 144)

## Notes and Other Information
- Located in src/backend/access/common/indextuple.c:241-455
- Uses a sophisticated offset caching strategy that stores calculated offsets in the tuple descriptor's attcacheoff field
- Handles three distinct cases based on the presence of nulls and variable-width attributes before the target attribute
- The null bitmap is located immediately after the IndexTupleData header
- For fixed-width columns without preceding nulls or variable-width attributes, the function pre-calculates and caches offsets for all leading columns
- [Variable](../V/Variable.md)-length attribute handling includes proper alignment considerations
- The caching mechanism improves performance significantly for repeated access to the same attributes across multiple tuples
- Comment indicates this approach was designed by "cim 5/4/91" as a performance optimization

## Simplified Source

```c
Datum nocache_index_getattr(IndexTuple tup, int attnum, TupleDesc tupleDesc) {
    char *tp;
    bits8 *bp = NULL;
    bool slow = false;
    int data_off;
    int off;

    data_off = IndexInfoFindDataOffset(tup->t_info);
    attnum--;  // Convert to 0-based indexing

    // Check if we need to handle nulls
    if (IndexTupleHasNulls(tup)) {
        bp = (bits8 *) ((char *) tup + sizeof(IndexTupleData));

        // Check for nulls before target attribute
        int byte = attnum >> 3;
        int finalbit = attnum & 0x07;

        if ((~bp[byte]) & ((1 << finalbit) - 1)) {
            slow = true;
        } else {
            // Check earlier bytes for nulls
            for (int i = 0; i < byte; i++) {
                if (bp[i] != 0xFF) {
                    slow = true;
                    break;
                }
            }
        }
    }

    tp = (char *) tup + data_off;

    // Fast path: no nulls or variable widths before target
    if (!slow) {
        Form_pg_attribute att = TupleDescAttr(tupleDesc, attnum);

        // Use cached offset if available
        if (att->attcacheoff >= 0)
            return fetchatt(att, tp + att->attcacheoff);

        // Check for variable-width attributes
        if (IndexTupleHasVarwidths(tup)) {
            for (int j = 0; j <= attnum; j++) {
                if (TupleDescAttr(tupleDesc, j)->attlen <= 0) {
                    slow = true;
                    break;
                }
            }
        }
    }

    // Calculate and cache fixed-width offsets
    if (!slow) {
        int natts = tupleDesc->natts;
        int j = 1;

        TupleDescAttr(tupleDesc, 0)->attcacheoff = 0;

        // Skip already cached offsets
        while (j < natts && TupleDescAttr(tupleDesc, j)->attcacheoff > 0)
            j++;

        off = TupleDescAttr(tupleDesc, j - 1)->attcacheoff +
              TupleDescAttr(tupleDesc, j - 1)->attlen;

        // Cache offsets for leading fixed-width columns
        for (; j < natts; j++) {
            Form_pg_attribute att = TupleDescAttr(tupleDesc, j);

            if (att->attlen <= 0) break;

            off = att_align_nominal(off, att->attalign);
            att->attcacheoff = off;
            off += att->attlen;
        }

        off = TupleDescAttr(tupleDesc, attnum)->attcacheoff;
    } else {
        // Slow path: walk tuple carefully handling nulls and variable widths
        bool usecache = true;
        off = 0;

        for (int i = 0;; i++) {
            Form_pg_attribute att = TupleDescAttr(tupleDesc, i);

            // Skip null attributes
            if (IndexTupleHasNulls(tup) && att_isnull(i, bp)) {
                usecache = false;
                continue;
            }

            // Use cached offset if available
            if (usecache && att->attcacheoff >= 0) {
                off = att->attcacheoff;
            } else if (att->attlen == -1) {
                // Variable-length attribute
                if (usecache && off == att_align_nominal(off, att->attalign))
                    att->attcacheoff = off;
                else {
                    off = att_align_pointer(off, att->attalign, -1, tp + off);
                    usecache = false;
                }
            } else {
                // Fixed-length attribute
                off = att_align_nominal(off, att->attalign);
                if (usecache) att->attcacheoff = off;
            }

            if (i == attnum) break;

            off = att_addlength_pointer(off, att->attlen, tp + off);

            if (usecache && att->attlen <= 0)
                usecache = false;
        }
    }

    return fetchatt(TupleDescAttr(tupleDesc, attnum), tp + off);
}
```