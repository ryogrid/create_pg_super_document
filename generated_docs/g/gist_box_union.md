# gist_box_union

## Location
[src/backend/access/gist/gistproc.c:164-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L164-L198)

## Overview
The GiST Union method for boxes that returns the minimal bounding box that encloses all the entries in the provided entry vector.

## Definition
```c
Datum gist_box_union(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the Union operation for the GiST (Generalized Search Tree) index structure when working with geometric box data types. The union operation is a fundamental GiST requirement that computes the minimal bounding box that encompasses all child entries. This is essential for maintaining the tree structure where internal nodes store bounding boxes that cover all entries in their subtrees.

The function iterates through all entries in the provided vector, starting with the first entry as the initial bounding box, then progressively adjusts this box to include each subsequent entry using the adjustBox helper function.

## Parameters / Member Variables
- `entryvec`: GistEntryVector pointer containing the collection of entries to compute the union for
- `sizep`: Integer pointer where the size of the result (sizeof(BOX)) will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetBoxP](../D/DatumGetBoxP.md): Extracts BOX pointer from Datum
  - [adjustBox](../a/adjustBox.md): Adjusts bounding box to include another box
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - memcpy: Memory copy function
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Location: src/backend/access/gist/gistproc.c:164-198
- This function is part of the GiST operator class for geometric box types
- The returned BOX is allocated using palloc and should be managed by PostgreSQL's memory context system
- The function follows PostgreSQL's function calling conventions using PG_FUNCTION_ARGS and PG_RETURN_POINTER

## Simplified Source

```c
Datum
gist_box_union(PG_FUNCTION_ARGS)
{
    GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
    int *sizep = (int *) PG_GETARG_POINTER(1);
    int numranges, i;
    BOX *cur, *pageunion;

    numranges = entryvec->n;

    // Allocate result box and initialize with first entry
    pageunion = (BOX *) palloc(sizeof(BOX));
    cur = DatumGetBoxP(entryvec->vector[0].key);
    memcpy(pageunion, cur, sizeof(BOX));

    // Adjust union box to include each subsequent entry
    for (i = 1; i < numranges; i++) {
        cur = DatumGetBoxP(entryvec->vector[i].key);
        adjustBox(pageunion, cur);
    }

    *sizep = sizeof(BOX);
    PG_RETURN_POINTER(pageunion);
}
```