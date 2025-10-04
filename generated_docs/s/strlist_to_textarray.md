# strlist_to_textarray

## Location
[src/backend/catalog/objectaddress.c:6043-6097](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L6043-L6097)

## Overview
Converts a PostgreSQL List of C-strings into a PostgreSQL TEXT array (ArrayType), providing a bridge between internal list representations and SQL array types.

## Definition
ArrayType *strlist_to_textarray(List *list)

## Detailed Description
This auxiliary function transforms a PostgreSQL List containing C-string elements into a TEXT array that can be used in SQL contexts. The function creates a temporary memory context to efficiently manage memory allocation for the array construction process, then builds the necessary Datum and null arrays required by PostgreSQL's array construction functions. It handles null string values by setting appropriate null flags in the resulting array.

The function operates by iterating through each list element, converting non-null C-strings to TEXT datums using CStringGetTextDatum(), and tracking null values appropriately. After collecting all elements, it uses construct_md_array() to build the final ArrayType structure with proper array metadata.

## Parameters / Member Variables
- list: A PostgreSQL List containing char* elements (C-strings) to be converted into a TEXT array. May contain NULL elements.

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate: Creates temporary memory context for array construction
  - ALLOCSET_DEFAULT_SIZES: Default memory context size parameters
  - [construct_md_array](../c/construct_md_array.md): Core PostgreSQL function to build multidimensional arrays
  - TYPALIGN_INT: Type alignment constant for integer alignment
  - [MemoryContextDelete](../M/MemoryContextDelete.md): Cleans up temporary memory context
  - CStringGetTextDatum: Converts C-string to PostgreSQL TEXT datum
  - [list_length](../l/list_length.md): Gets the length of a PostgreSQL List
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md): Switches memory contexts

- Called from (representative examples):
  - [pg_identify_object_as_address](../p/pg_identify_object_as_address.md): Object identification system
  - [pg_event_trigger_dropped_objects](../p/pg_event_trigger_dropped_objects.md): Event trigger handling for dropped objects
  - [fill_hba_line](../f/fill_hba_line.md): Host-based authentication configuration processing

## Notes and Other Information
- Uses a temporary memory context to simplify memory management and avoid individual pfree() calls
- Handles null string values gracefully by setting corresponding null flags
- Returns a one-dimensional TEXT array with lower bound of 1 (PostgreSQL convention)
- The resulting array uses TEXTOID type with integer alignment
- Memory-efficient approach that cleans up temporary allocations automatically

## Simplified Source

```c
ArrayType *
strlist_to_textarray(List *list)
{
    ArrayType  *arr;
    Datum      *datums;
    bool       *nulls;
    int         j = 0;
    ListCell   *cell;
    MemoryContext memcxt;
    MemoryContext oldcxt;
    int         lb[1];

    // Create temporary memory context for array construction
    memcxt = AllocSetContextCreate(CurrentMemoryContext,
                                   "strlist to array",
                                   ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(memcxt);

    // Allocate arrays for datums and null flags
    datums = (Datum *) palloc(sizeof(Datum) * list_length(list));
    nulls = palloc(sizeof(bool) * list_length(list));

    // Convert each list element to a TEXT datum
    foreach(cell, list)
    {
        char *name = lfirst(cell);

        if (name)
        {
            nulls[j] = false;
            datums[j++] = CStringGetTextDatum(name);
        }
        else
            nulls[j] = true;
    }

    MemoryContextSwitchTo(oldcxt);

    // Construct the TEXT array with lower bound of 1
    lb[0] = 1;
    arr = construct_md_array(datums, nulls, 1, &j,
                            lb, TEXTOID, -1, false, TYPALIGN_INT);

    // Clean up temporary memory context
    MemoryContextDelete(memcxt);

    return arr;
}
```