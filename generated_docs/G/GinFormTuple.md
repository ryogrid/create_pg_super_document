# GinFormTuple

## Location
[src/backend/access/gin/ginentrypage.c:44-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L44-L161)

## Overview
Forms a tuple for entry tree in PostgreSQL's GIN (Generalized Inverted Index) access method, creating an IndexTuple from key data and posting list information.

## Definition

```c
IndexTuple
GinFormTuple(GinState *ginstate,
			 OffsetNumber attnum, Datum key, GinNullCategory category,
			 Pointer data, Size dataSize, int nipd,
			 bool errorTooBig)
```
## Detailed Description
The GinFormTuple function creates an IndexTuple specifically for GIN entry tree pages. It builds the basic tuple structure containing optional column number and key datum, then adds space for the posting list. The function handles both single-column and multi-column GIN indexes, manages null categories, and ensures the resulting tuple doesn't exceed maximum size limits.

The function is designed primarily for leaf-level key entries containing posting lists, but can be adapted for posting-tree entries, non-leaf entries, or pending-list entries by passing dataSize = 0 and overwriting t_tid fields as necessary.

## Parameters / Member Variables
- : GIN state structure containing index metadata and tuple descriptors
- : Attribute number (column number) for the key
- : The key datum to be stored in the tuple
- : GIN null category for handling null values and special cases
- : Pointer to posting list data (can be NULL)
- : Size of the posting list data in bytes
- : Number of items in posting list
- : If true, throws error when tuple is too big; if false, returns NULL

## Dependencies
- Functions called/Symbols referenced:
  - [index_form_tuple](../i/index_form_tuple.md): Creates basic IndexTuple from datums and nulls arrays
  - [UInt16GetDatum](../U/UInt16GetDatum.md): Converts attribute number to Datum
  - IndexTupleSize: Gets current size of index tuple
  - IndexTupleHasNulls: Checks if tuple has null values
  - GinCategoryOffset: Calculates offset for null category storage
  - GinSetPostingOffset: Sets offset to posting list in tuple
  - GinSetNPosting: Sets number of posting list items
  - GinGetPosting: Gets pointer to posting list data in tuple
  - GinSetNullCategory: Sets null category byte in tuple
  - [repalloc](../r/repalloc.md): Reallocates tuple memory if size changes

- Called from (representative examples):
  - [ginHeapTupleFastCollect](../g/ginHeapTupleFastCollect.md): Fast collection of heap tuples during GIN operations
  - [addItemPointersToLeafTuple](../a/addItemPointersToLeafTuple.md): Adding item pointers to leaf tuples during insertion
  - [buildFreshLeafTuple](../b/buildFreshLeafTuple.md): Building new leaf tuples
  - [ginVacuumEntryPage](../g/ginVacuumEntryPage.md): Vacuum operations on entry pages

## Notes and Other Information
- The function handles both single-column and multi-column GIN indexes through ginstate->oneCol
- Memory alignment is carefully managed using SHORTALIGN and MAXALIGN macros
- The function can return NULL instead of throwing an error when tuple size exceeds limits
- Space allocation includes padding considerations and ensures proper alignment
- The posting list data can be copied later if data parameter is NULL
- Category bytes are inserted for non-normal keys to handle null values and special cases

## Simplified Source

```c
IndexTuple
GinFormTuple(GinState *ginstate,
             OffsetNumber attnum, Datum key, GinNullCategory category,
             Pointer data, Size dataSize, int nipd,
             bool errorTooBig)
{
    Datum datums[2];
    bool isnull[2];
    IndexTuple itup;
    uint32 newsize;

    // Build basic tuple: optional column number, plus key datum
    if (ginstate->oneCol) {
        datums[0] = key;
        isnull[0] = (category != GIN_CAT_NORM_KEY);
    } else {
        datums[0] = UInt16GetDatum(attnum);
        isnull[0] = false;
        datums[1] = key;
        isnull[1] = (category != GIN_CAT_NORM_KEY);
    }

    itup = index_form_tuple(ginstate->tupdesc[attnum - 1], datums, isnull);

    // Calculate size needed including space for category byte if needed
    newsize = IndexTupleSize(itup);

    if (IndexTupleHasNulls(itup)) {
        uint32 minsize;
        Assert(category != GIN_CAT_NORM_KEY);
        minsize = GinCategoryOffset(itup, ginstate) + sizeof(GinNullCategory);
        newsize = Max(newsize, minsize);
    }

    newsize = SHORTALIGN(newsize);

    // Set posting list metadata
    GinSetPostingOffset(itup, newsize);
    GinSetNPosting(itup, nipd);

    // Add space for posting list data
    newsize += dataSize;
    newsize = MAXALIGN(newsize);

    // Check size limit
    if (newsize > GinMaxItemSize) {
        if (errorTooBig)
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
                            (Size) newsize, (Size) GinMaxItemSize,
                            RelationGetRelationName(ginstate->index))));
        pfree(itup);
        return NULL;
    }

    // Resize tuple if needed
    if (newsize != IndexTupleSize(itup)) {
        itup = repalloc(itup, newsize);

        // Clear new space for compatibility
        memset((char *) itup + IndexTupleSize(itup),
               0, newsize - IndexTupleSize(itup));

        // Update size in tuple header
        itup->t_info &= ~INDEX_SIZE_MASK;
        itup->t_info |= newsize;
    }

    // Copy posting list data if provided
    if (data) {
        char *ptr = GinGetPosting(itup);
        memcpy(ptr, data, dataSize);
    }

    // Insert category byte for non-normal keys
    if (category != GIN_CAT_NORM_KEY) {
        Assert(IndexTupleHasNulls(itup));
        GinSetNullCategory(itup, ginstate, category);
    }

    return itup;
}
```