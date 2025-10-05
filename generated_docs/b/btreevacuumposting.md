# btreevacuumposting

## Location
[src/backend/access/nbtree/nbtree.c:1408-1456](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L1408-L1456)

## Overview
Determines which TIDs need to be preserved in a posting list tuple during vacuum, creating metadata for partial tuple updates when some TIDs are dead.

## Definition
```c
static BTVacuumPosting btreevacuumposting(BTVacState *vstate, IndexTuple posting, OffsetNumber updatedoffset, int *nremaining)
```

## Detailed Description
The `btreevacuumposting` function analyzes a posting list tuple during vacuum operations to determine which table TIDs (tuple identifiers) should remain and which should be deleted. Posting list tuples are a space optimization in B-tree indexes that allow a single index tuple to reference multiple heap table rows with the same key values.

The function iterates through all TIDs in the posting list, calling the vacuum callback function to determine if each TID should be deleted. When dead TIDs are found, it constructs a BTVacuumPosting metadata structure containing information needed to update the posting list tuple by removing only the dead TIDs while preserving the live ones.

As an optimization, the function returns NULL when no TIDs need to be deleted, avoiding unnecessary memory allocation in the common case where the entire posting list remains unchanged.

## Parameters / Member Variables
- `vstate`: BTVacState structure containing vacuum state and callback function
- `posting`: The posting list IndexTuple being analyzed
- `updatedoffset`: Offset number of this tuple on its page (for update metadata)
- `nremaining`: Output parameter set to the number of TIDs that should remain in the posting list

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md) (get number of TIDs in posting list)
  - [BTreeTupleGetPosting](../B/BTreeTupleGetPosting.md) (get array of TIDs from posting list)
  - [palloc](../p/palloc.md) (allocate memory for BTVacuumPosting structure)
  - vstate->callback (vacuum callback to test if TID should be deleted)
- Called from:
  - [btvacuumpage](btvacuumpage.md) (during leaf page vacuum processing)

## Notes and Other Information
- Returns NULL when no changes are needed (optimization to avoid memory allocation)
- The BTVacuumPosting structure tracks both the original tuple and which TID positions to delete
- Critical for handling partial deletions in posting list tuples without losing live references
- Part of PostgreSQL's optimization to reduce index size when multiple heap tuples have identical key values
- Memory allocated for BTVacuumPosting is managed by the caller and freed after tuple update

## Simplified Source

```c
static BTVacuumPosting btreevacuumposting(BTVacState *vstate, IndexTuple posting,
                                         OffsetNumber updatedoffset, int *nremaining) {
    int live = 0;
    int nitem = BTreeTupleGetNPosting(posting);
    ItemPointer items = BTreeTupleGetPosting(posting);
    BTVacuumPosting vacposting = NULL;

    // Check each TID in posting list via callback
    for (int i = 0; i < nitem; i++) {
        if (!vstate->callback(items + i, vstate->callback_state)) {
            // Live TID - keep it
            live++;
        } else if (vacposting == NULL) {
            // First dead TID found - allocate metadata structure
            vacposting = palloc(offsetof(BTVacuumPostingData, deletetids) +
                              nitem * sizeof(uint16));

            vacposting->itup = posting;
            vacposting->updatedoffset = updatedoffset;
            vacposting->ndeletedtids = 0;
            vacposting->deletetids[vacposting->ndeletedtids++] = i;
        } else {
            // Additional dead TID
            vacposting->deletetids[vacposting->ndeletedtids++] = i;
        }
    }

    *nremaining = live;
    return vacposting;  // NULL if no deletions needed
}
```