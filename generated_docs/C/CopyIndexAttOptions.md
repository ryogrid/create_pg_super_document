# CopyIndexAttOptions

## Location
[src/backend/utils/cache/relcache.c:5876-5895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5876-L5895)

## Overview
Creates a deep copy of an array of index attribute options, ensuring each bytea structure is properly duplicated in memory.

## Definition

```c
static bytea **
CopyIndexAttOptions(bytea **srcopts, int natts)
```
## Detailed Description
This is a static utility function that creates a complete copy of an array of index attribute options. Each element in the source array is a  pointer that contains attribute-specific options for index columns.

The function allocates a new array of  pointers and then iterates through each element of the source array. For each non-NULL option, it performs a deep copy using PostgreSQL's  function, which ensures that the copied data is completely independent of the original.

The copying process uses:
-  to convert the bytea pointer to a Datum
-  with  and  indicating variable-length, pass-by-reference data
-  to convert the copied Datum back to a bytea pointer

This ensures that modifications to the copied options won't affect the original options and vice versa.

## Parameters / Member Variables
- `**srcopts`: Source array of bytea pointers containing the original index attribute options
- `natts`: Number of attributes (length of the array)
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [datumCopy](../d/datumCopy.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)/DatumGetPointer
- Called from (representative examples):
  - [RelationGetIndexAttOptions](../R/RelationGetIndexAttOptions.md)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Properly handles NULL option entries by preserving them as NULL in the copy
- Uses PostgreSQL's datum copying mechanism to ensure proper deep copying of variable-length data
- The returned array should be freed appropriately when no longer needed
- Essential for maintaining data integrity when index attribute options need to be cached or transferred between contexts

## Simplified Source

```c
static bytea **CopyIndexAttOptions(bytea **srcopts, int natts) {
    // Allocate array for copied options
    bytea **opts = palloc(sizeof(*opts) * natts);

    // Copy each option, handling NULL entries
    for (int i = 0; i < natts; i++) {
        bytea *opt = srcopts[i];

        if (opt == NULL) {
            opts[i] = NULL;
        } else {
            // Deep copy the bytea structure
            opts[i] = (bytea *) DatumGetPointer(
                datumCopy(PointerGetDatum(opt), false, -1)
            );
        }
    }

    return opts;
}
```