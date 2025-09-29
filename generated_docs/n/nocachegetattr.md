# nocachegetattr

## Location
[src/backend/access/common/heaptuple.c:519-722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L519-L722)

## Overview
nocachegetattr extracts attribute values from heap tuples when cached offsets cannot be used, implementing an optimization strategy that caches computed attribute offsets in the tuple descriptor for future use.

## Definition

```c
Datum
nocachegetattr(HeapTuple tup,
			   int attnum,
			   TupleDesc tupleDesc)
```
## Detailed Description
nocachegetattr is a performance-critical function called from fastgetattr() when cached offsets are not available and the requested attribute is not null. It handles the complex task of locating attribute data within a tuple while dealing with variable-length attributes, null values, and alignment requirements.

The function implements a sophisticated caching strategy that stores computed offsets in the tuple descriptor's attribute metadata (attcacheoff field). This allows subsequent accesses to the same attributes in other tuples using the same tuple descriptor to skip the expensive offset calculation.

The function handles three main scenarios:
1. No nulls and no variable-width attributes - fastest path with simple offset calculation
2. Has nulls or variable-width attributes after the target attribute - can still use some optimizations
3. Has nulls or variable-width attributes before the target attribute - requires careful traversal

Key optimizations include:
- Checking for nulls in preceding attributes using bitwise operations
- Bulk initialization of cached offsets for all leading fixed-width columns
- Strategic caching decisions based on alignment requirements for variable-length attributes

## Parameters / Member Variables
- : HeapTuple containing the tuple data to extract from
- : Attribute number to extract (1-based indexing)
- : TupleDesc describing the tuple structure and containing cached offset information

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleNoNulls, HeapTupleHasVarWidth, HeapTupleHasNulls (tuple property checks)
  - TupleDescAttr (access tuple descriptor attributes)
  - [att_isnull](../a/att_isnull.md) (check null bitmap)
  - fetchatt (extract final attribute value)
  - att_align_nominal, att_align_pointer (handle data alignment)
  - att_addlength_pointer (calculate variable-length attribute sizes)
- Called from (representative examples):
  - [fastgetattr](../f/fastgetattr.md) (primary caller - inline macro)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Only called from fastgetattr() when cached offsets are unavailable and the value is not null
- Implements a crucial performance optimization by caching attribute offsets in the tuple descriptor
- The caching strategy significantly improves performance for queries processing many tuples with the same structure
- Must handle complex alignment requirements for different data types
- Coordinates with heap_deform_tuple and nocache_index_getattr which use similar logic
- The offset caching is conservative for variable-length attributes - only caches when alignment is guaranteed
- Uses bit manipulation for efficient null checking in the tuple's null bitmap
- Critical for PostgreSQL's tuple access performance, especially for wide tables with many attributes
- The function converts from 1-based attribute numbering (external interface) to 0-based internal indexing
- Balances between computation cost and cache effectiveness to optimize overall system performance

## Simplified Source

```c
Datum nocachegetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc) {
    HeapTupleHeader td = tup->t_data;
    char *tp = (char *) td + td->t_hoff;  // Data start
    bits8 *bp = td->t_bits;               // Null bitmap
    bool slow = false;
    int off;

    attnum--;  // Convert to 0-based indexing

    // Check if there are nulls before target attribute
    if (!HeapTupleNoNulls(tup)) {
        // Use bit manipulation to check for nulls before target
        int byte = attnum >> 3;
        int finalbit = attnum & 0x07;
        if ((~bp[byte]) & ((1 << finalbit) - 1)) {
            slow = true;  // Found nulls before target
        } else {
            // Check earlier bytes for any nulls
            for (int i = 0; i < byte; i++) {
                if (bp[i] != 0xFF) {
                    slow = true;
                    break;
                }
            }
        }
    }

    if (!slow) {
        // Fast path: no nulls before target
        Form_pg_attribute att = TupleDescAttr(tupleDesc, attnum);

        // Use cached offset if available
        if (att->attcacheoff >= 0) {
            return fetchatt(att, tp + att->attcacheoff);
        }

        // Check for variable-width attributes up to target
        if (HeapTupleHasVarWidth(tup)) {
            for (int j = 0; j <= attnum; j++) {
                if (TupleDescAttr(tupleDesc, j)->attlen <= 0) {
                    slow = true;
                    break;
                }
            }
        }

        // If still fast path, cache offsets for all fixed-width attributes
        if (!slow) {
            // Initialize cached offsets for leading fixed-width columns
            off = 0;
            for (int j = 0; j < tupleDesc->natts && j <= attnum; j++) {
                Form_pg_attribute att = TupleDescAttr(tupleDesc, j);
                if (att->attlen <= 0) break;

                off = att_align_nominal(off, att->attalign);
                att->attcacheoff = off;
                off += att->attlen;
            }
            off = TupleDescAttr(tupleDesc, attnum)->attcacheoff;
        }
    }

    if (slow) {
        // Slow path: walk through tuple carefully
        off = 0;
        bool usecache = true;

        for (int i = 0; i <= attnum; i++) {
            Form_pg_attribute att = TupleDescAttr(tupleDesc, i);

            // Skip null attributes
            if (HeapTupleHasNulls(tup) && att_isnull(i, bp)) {
                usecache = false;
                continue;
            }

            // Handle alignment and caching
            if (usecache && att->attcacheoff >= 0) {
                off = att->attcacheoff;
            } else {
                off = att_align_nominal(off, att->attalign);
                if (usecache) att->attcacheoff = off;
            }

            if (i == attnum) break;

            // Advance past this attribute
            off = att_addlength_pointer(off, att->attlen, tp + off);
            if (att->attlen <= 0) usecache = false;
        }
    }

    return fetchatt(TupleDescAttr(tupleDesc, attnum), tp + off);
}
```