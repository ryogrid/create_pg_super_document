# _hash_pgaddtup

## Location
[src/backend/access/hash/hashinsert.c:274-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashinsert.c#L274-L330)

## Overview
The  function adds a single index tuple to a specific page in a hash index while maintaining the page's hashkey ordering.

## Definition

```c
OffsetNumber
_hash_pgaddtup(Relation rel, Buffer buf, Size itemsize, IndexTuple itup,
			   bool appendtup)
```
## Detailed Description
This function is responsible for inserting a single tuple into a hash index page at the correct position to maintain hashkey ordering. It provides two insertion modes:

1. **Ordered insertion**: When  is false, the function performs a binary search to find the correct insertion position based on the tuple's hashkey value, ensuring the page remains sorted.

2. **Append insertion**: When  is true, the function appends the tuple at the end of the page. This optimization is used when the caller guarantees that the new tuple's hashkey is greater than or equal to all existing tuples on the page.

The function includes validation to ensure the buffer contains a valid hash bucket or overflow page and performs assertion checking in debug builds to verify the hashkey ordering constraint when appending.

## Parameters / Member Variables
- `rel`: The hash index relation
- `buf`: Buffer containing the target page (must be pinned and write-locked)
- `itemsize`: Size of the tuple to be inserted
- `itup`: The index tuple to be inserted
- `appendtup`: Boolean flag indicating whether to append at end (true) or search for correct position (false)
## Dependencies
- Functions called/Symbols referenced:
  - : Validate page type (bucket or overflow page)
  - : Extract hashkey from index tuple
  - : Binary search for insertion position
  - : Get highest offset number on page
  - , : Access existing tuples for validation
  - : Add the tuple to the page
  - : Get relation name for error reporting

- Called from (representative examples):
  - : Main tuple insertion function

## Notes and Other Information
- The caller must hold both a pin and write lock on the target buffer before calling this function
- The function does not write the page to disk; that responsibility lies with the caller
- In debug builds (USE_ASSERT_CHECKING), the function validates that appended tuples maintain hashkey ordering
- Returns InvalidOffsetNumber and raises an ERROR if the page addition fails
- The function maintains the critical invariant that hash index pages are sorted by hashkey value
- The  optimization is particularly useful during bulk loading or when inserting tuples in hashkey order

## Simplified Source

```c
// Add a tuple to a particular page in the hash index.
// Preserves hashkey ordering on the page.
OffsetNumber _hash_pgaddtup(Relation rel, Buffer buf, Size itemsize,
                           IndexTuple itup, bool appendtup) {
    OffsetNumber itup_off;
    Page page;

    // Validate buffer contains a hash bucket or overflow page
    _hash_checkpage(rel, buf, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE);
    page = BufferGetPage(buf);

    if (appendtup) {
        // Append at end (caller guarantees hashkey ordering)
        itup_off = PageGetMaxOffsetNumber(page) + 1;

        // Debug check: ensure new tuple's hashkey >= last tuple's hashkey
        #ifdef USE_ASSERT_CHECKING
        if (PageGetMaxOffsetNumber(page) > 0) {
            ItemId itemid = PageGetItemId(page, PageGetMaxOffsetNumber(page));
            IndexTuple lasttup = (IndexTuple) PageGetItem(page, itemid);
            Assert(_hash_get_indextuple_hashkey(lasttup) <=
                   _hash_get_indextuple_hashkey(itup));
        }
        #endif
    } else {
        // Find correct position to maintain hashkey ordering
        uint32 hashkey = _hash_get_indextuple_hashkey(itup);
        itup_off = _hash_binsearch(page, hashkey);
    }

    // Add tuple to page at the determined offset
    if (PageAddItem(page, (Item) itup, itemsize, itup_off, false, false)
        == InvalidOffsetNumber)
        elog(ERROR, "failed to add index item to \"%s\"",
             RelationGetRelationName(rel));

    return itup_off;
}
```