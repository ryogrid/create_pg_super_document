# fallbackSplit

## Location
src/backend/access/gist/gistproc.c: 216 - 276

## Overview
A simple fallback splitting algorithm for GiST indexes that divides entries into two roughly equal groups when more sophisticated splitting methods fail.

## Definition
```c
static void fallbackSplit(GistEntryVector *entryvec, GIST_SPLITVEC *v)
```

## Detailed Description
This function implements a trivial splitting strategy for GiST (Generalized Search Tree) indexes when dealing with geometric box data types. It serves as a fallback mechanism when more sophisticated splitting algorithms cannot be applied or fail. The algorithm simply divides the entries into two groups: the first half goes to the left page, and the second half goes to the right page.

For each group, the function computes the union (minimal bounding box) of all entries in that group using the adjustBox helper function. This ensures that each resulting page has a proper bounding box that covers all its entries, maintaining the GiST tree invariants.

## Parameters / Member Variables
- `entryvec`: GistEntryVector containing all entries that need to be split
- `v`: GIST_SPLITVEC structure that will be populated with the split result, including:
  - `spl_left`: Array of offset numbers for entries going to the left page
  - `spl_right`: Array of offset numbers for entries going to the right page
  - `spl_nleft`: Number of entries in the left group
  - `spl_nright`: Number of entries in the right group
  - `spl_ldatum`: Union bounding box for the left group
  - `spl_rdatum`: Union bounding box for the right group

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetBoxP: Extracts BOX pointer from Datum
  - adjustBox: Adjusts bounding box to include another box
  - BoxPGetDatum: Converts BOX pointer to Datum
  - palloc: PostgreSQL memory allocation function
  - FirstOffsetNumber: Constant for the first valid offset number
  - OffsetNumberNext: Macro to get the next offset number
- Called from (representative examples):
  - gist_box_picksplit: Uses this as a fallback splitting method

## Notes and Other Information
- Location: src/backend/access/gist/gistproc.c:216-276
- This is a static function, only accessible within the same source file
- The algorithm is simple but not optimal - it doesn't consider the spatial relationships between boxes
- Used when more sophisticated splitting algorithms (like R-tree quadratic split) are not applicable
- Ensures roughly balanced splits by dividing entries in half
- Maintains GiST invariants by computing proper bounding boxes for both resulting groups