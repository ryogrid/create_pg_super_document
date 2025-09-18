# findComments

## Location
src/bin/pg_dump/pg_dump.c: 10360 - 10436

## Overview
Performs a binary search on the global comments array to find all comment entries associated with a specific database object identified by classoid and objoid.

## Definition


## Detailed Description
This function implements an efficient binary search algorithm to locate comment entries for a given database object in the pre-loaded, sorted global comments array. The search finds all matching entries because multiple comments can be associated with a single object (e.g., table comments and column comments share the same classoid/objoid but have different objsubid values).

The algorithm works in two phases:
1. **Binary Search**: Locates any matching entry using standard binary search on classoid/objoid pairs
2. **Range Expansion**: Once a match is found, expands both directions to find all entries with the same classoid/objoid combination

The function ensures that all sub-object comments (different objsubid values) are returned together in a single search operation.

## Parameters / Member Variables
- : OID of the catalog table (e.g., pg_class, pg_proc) that contains the object
- : OID of the specific object within that catalog
- : Output parameter that will point to the first matching CommentItem in the array

## Dependencies
- Functions called/Symbols referenced:
  - CommentItem (structure type)
- Called from (representative examples):
  - [dumpCommentExtended](../d/dumpCommentExtended.md)
  - [dumpTableComment](../d/dumpTableComment.md)
  - [dumpCompositeTypeColComments](../d/dumpCompositeTypeColComments.md)

## Notes and Other Information
- Relies on the global  array being pre-sorted by classoid, then objoid
- Returns the count of matching items found, or 0 if no matches
- The returned items pointer points directly into the global comments array
- Uses efficient binary search with O(log n) time complexity for initial lookup
- The range expansion phase runs in O(k) time where k is the number of matching items
- All matching entries are guaranteed to be contiguous in the sorted array
- Does not perform any memory allocation; returns pointers to existing data structures