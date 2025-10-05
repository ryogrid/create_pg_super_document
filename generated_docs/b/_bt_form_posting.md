# _bt_form_posting

## Location
[src/backend/access/nbtree/nbtdedup.c:864-923](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L864-L923)

## Overview
Builds a posting list tuple or standard non-pivot tuple based on a base index tuple and an array of heap TIDs, handling both single TID and multiple TID cases.

## Definition
```c
IndexTuple _bt_form_posting(IndexTuple base, ItemPointer htids, int nhtids)
```

## Detailed Description
This function creates either a posting list tuple (when nhtids > 1) or a standard non-pivot tuple (when nhtids == 1) from a base index tuple and an array of heap TIDs. It's a key component in PostgreSQL's B-tree deduplication system that consolidates multiple identical index keys into a single tuple with multiple heap TID references.

The function follows important conventions:
- Posting lists start at MAXALIGN()'d offsets rather than SHORTALIGN()'d offsets, consistent with suffix truncation in pivot tuples
- When nhtids == 1, it builds a standard non-pivot tuple without a posting list, since posting list tuples must always contain multiple TIDs
- The deduplication process always reduces the final MAXALIGN()'d size of the entire tuple

The implementation handles two distinct cases:
1. **Multiple TIDs (nhtids > 1)**: Creates a posting list tuple with the key data followed by an array of ItemPointers
2. **Single TID (nhtids == 1)**: Creates a standard tuple with the TID stored in the tuple header's t_tid field

The function performs careful size calculations and memory allocation, ensuring proper alignment and validation of the resulting tuple structure.

## Parameters / Member Variables
- `base`: The base index tuple to use as the foundation (key data source)
- `htids`: Array of heap TIDs to include in the posting list (must be unique and in ascending order)
- `nhtids`: Number of heap TIDs in the htids array (must be > 0 and <= PG_UINT16_MAX)

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [BTreeTupleGetPostingOffset](../B/BTreeTupleGetPostingOffset.md)
  - IndexTupleSize
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [palloc0](../p/palloc0.md)
  - [BTreeTupleSetPosting](../B/BTreeTupleSetPosting.md)
  - [BTreeTupleGetPosting](../B/BTreeTupleGetPosting.md)
  - [_bt_posting_valid](_bt_posting_valid.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Called from:
  - [_bt_dedup_finish_pending](_bt_dedup_finish_pending.md)
  - [_bt_sort_dedup_finish_pending](_bt_sort_dedup_finish_pending.md)

## Notes and Other Information
- This is a non-static function in the nbtdedup.c module, making it available to other parts of the B-tree system
- The caller must ensure that the htids array contains unique TIDs in ascending order
- Existing heap TIDs from the base tuple are not automatically included; they must be explicitly provided in the htids array
- The function uses palloc0() for memory allocation, matching the behavior of index_form_tuple()
- The MAXALIGN() convention simplifies space accounting for both deduplication and page splitting operations
- Posting list tuples can never have a single heap TID by design, ensuring deduplication always provides space savings
- Located at src/backend/access/nbtree/nbtdedup.c:864-923

## Simplified Source

```c
IndexTuple _bt_form_posting(IndexTuple base, ItemPointer htids, int nhtids) {
    Assert(!BTreeTupleIsPivot(base));
    Assert(nhtids > 0 && nhtids <= PG_UINT16_MAX);

    // Determine key size (without any existing posting list)
    uint32 keysize;
    if (BTreeTupleIsPosting(base))
        keysize = BTreeTupleGetPostingOffset(base);
    else
        keysize = IndexTupleSize(base);

    // Calculate final tuple size
    uint32 newsize;
    if (nhtids > 1)
        newsize = MAXALIGN(keysize + nhtids * sizeof(ItemPointerData));
    else
        newsize = keysize; // Standard tuple size for single TID

    // Allocate and initialize new tuple
    IndexTuple itup = palloc0(newsize);
    memcpy(itup, base, keysize); // Copy key data
    itup->t_info = (itup->t_info & ~INDEX_SIZE_MASK) | newsize;

    if (nhtids > 1) {
        // Create posting list tuple
        BTreeTupleSetPosting(itup, nhtids, keysize);
        memcpy(BTreeTupleGetPosting(itup), htids,
               sizeof(ItemPointerData) * nhtids);
        Assert(_bt_posting_valid(itup));
    } else {
        // Create standard non-pivot tuple
        itup->t_info &= ~INDEX_ALT_TID_MASK;
        ItemPointerCopy(htids, &itup->t_tid);
        Assert(ItemPointerIsValid(&itup->t_tid));
    }

    return itup;
}
```