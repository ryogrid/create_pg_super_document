# ExecInitJunkFilterConversion

## Location
[src/backend/executor/execJunk.c:137-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execJunk.c#L137-L209)

## Overview
Initializes a JunkFilter for rowtype conversions where the target tuple descriptor is explicitly provided, handling cases with deleted columns and ensuring proper mapping between original and converted tuple structures.

## Definition
```c
JunkFilter *ExecInitJunkFilterConversion(List *targetList, TupleDesc cleanTupType, TupleTableSlot *slot)
```

## Detailed Description
ExecInitJunkFilterConversion is a specialized version of ExecInitJunkFilter designed for rowtype conversion scenarios. Unlike the standard version that infers the clean tuple descriptor from the target list, this function accepts an explicit target tuple descriptor that may contain deleted columns.

The function is particularly important for handling schema evolution scenarios where columns have been dropped from tables. It creates a mapping that accounts for both junk attributes (which should be filtered) and deleted attributes (which should be represented as NULLs in the output).

Key differences from ExecInitJunkFilter:
1. Accepts a pre-computed cleanTupType instead of deriving it
2. Uses palloc0() to zero-initialize the mapping array, ensuring deleted columns map to zero
3. Handles deleted columns by checking the attisdropped flag and leaving corresponding map entries as zero
4. Assumes the caller has validated that non-deleted columns align with non-junk target list entries

## Parameters / Member Variables
- `targetList`: List of TargetEntry nodes representing the query's target list with potential junk attributes
- `cleanTupType`: Pre-computed tuple descriptor for the target "clean" tuple, possibly containing deleted columns
- `slot`: Optional TupleTableSlot to use for clean tuples; if NULL, a new virtual slot is created

## Dependencies
- Functions called/Symbols referenced:
  - ExecSetSlotDescriptor: Sets tuple descriptor for existing slot
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md): Creates new virtual tuple slot if none provided
  - [list_head](../l/list_head.md): Gets first cell of target list for iteration
  - [lnext](../l/lnext.md): Advances to next cell in target list during mapping creation
  - [JunkFilter](../J/JunkFilter.md): The result structure type
- Called from (representative examples):
  - [init_sql_fcache](../i/init_sql_fcache.md): For SQL function caching with type conversion needs

## Notes and Other Information
- Designed specifically for rowtype conversion scenarios where schema changes may have occurred
- Uses palloc0() instead of palloc() to ensure deleted column mappings are properly zeroed
- The mapping algorithm assumes caller has verified that non-deleted columns correspond to non-junk target entries
- Zero entries in cleanMap indicate deleted columns that should produce NULL values in output
- Critical for maintaining data integrity during schema evolution and type conversion operations

## Simplified Source

```c
JunkFilter *ExecInitJunkFilterConversion(List *targetList, TupleDesc cleanTupType, TupleTableSlot *slot)
{
    JunkFilter *junkfilter;
    int cleanLength;
    AttrNumber *cleanMap;
    ListCell *t;
    int i;

    // Step 1: Set up the result slot with the clean tuple descriptor
    if (slot) {
        ExecSetSlotDescriptor(slot, cleanTupType);
    } else {
        slot = MakeSingleTupleTableSlot(cleanTupType, &TTSOpsVirtual);
    }

    // Step 2: Build mapping from clean tuple positions to original tuple positions
    cleanLength = cleanTupType->natts;

    if (cleanLength > 0) {
        // Zero-initialize mapping array (zeros indicate deleted/NULL columns)
        cleanMap = (AttrNumber *) palloc0(cleanLength * sizeof(AttrNumber));

        t = list_head(targetList);

        // For each attribute in the clean tuple
        for (i = 0; i < cleanLength; i++) {
            if (TupleDescAttr(cleanTupType, i)->attisdropped) {
                continue;  // Deleted column: map entry remains zero
            }

            // Find next non-junk target entry
            for (;;) {
                TargetEntry *tle = lfirst(t);
                t = lnext(targetList, t);

                if (!tle->resjunk) {
                    cleanMap[i] = tle->resno;  // Map to original position
                    break;
                }
            }
        }
    } else {
        cleanMap = NULL;  // No attributes to map
    }

    // Step 3: Create and initialize the JunkFilter structure
    junkfilter = makeNode(JunkFilter);
    junkfilter->jf_targetList = targetList;
    junkfilter->jf_cleanTupType = cleanTupType;
    junkfilter->jf_cleanMap = cleanMap;
    junkfilter->jf_resultSlot = slot;

    return junkfilter;
}
```