# cmpEntries

## Location
[src/backend/access/gin/ginutil.c:443-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L443-L482)

## Overview
A static comparison function used for sorting key entries in GIN indexes, handling both regular data values and NULL values with appropriate ordering semantics.

## Definition

```c
static int
cmpEntries(const void *a, const void *b, void *arg)
```
## Detailed Description
The  function is a comparison function specifically designed for sorting  structures in GIN (Generalized Inverted Index) operations. It implements a three-way comparison that returns negative, zero, or positive values to indicate the relative ordering of two key entries. The function has special handling for NULL values, placing them after non-NULL values in the sort order (NULL > not-NULL). For non-NULL values, it delegates the actual comparison to a user-provided comparison function via . Additionally, it tracks whether duplicate entries are encountered during the sorting process by setting a flag in the argument structure.

## Parameters / Member Variables
- : Pointer to the first  structure to compare
- : Pointer to the second  structure to compare  
- : Pointer to a  structure containing the comparison function and collation information, plus a flag for tracking duplicates

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type for the entries being compared)
  -  (structure type for the comparison context)
  -  (macro to extract int32 from Datum)
  -  (function to call the actual comparison function with collation)
- Called from (representative examples):
  -  (uses this as qsort comparison function)

## Notes and Other Information
- This is a static function internal to the GIN utility module
- Implements NULL-last ordering semantics (NULLs sort after non-NULLs)
- Detects and flags duplicate entries during sorting via the  field
- Used as a callback function for  operations on key entry arrays
- The actual data comparison is delegated to a configurable comparison function stored in the argument structure

## Simplified Source

```c
static int cmpEntries(const void *a, const void *b, void *arg) {
    const keyEntryData *aa = (const keyEntryData *) a;
    const keyEntryData *bb = (const keyEntryData *) b;
    cmpEntriesArg *data = (cmpEntriesArg *) arg;
    int res;

    // Handle NULL comparisons (NULL > not-NULL)
    if (aa->isnull) {
        res = bb->isnull ? 0 : 1;
    } else if (bb->isnull) {
        res = -1;
    } else {
        // Compare non-NULL values using provided function
        res = DatumGetInt32(FunctionCall2Coll(data->cmpDatumFunc,
                                             data->collation,
                                             aa->datum, bb->datum));
    }

    // Track if duplicates are found
    if (res == 0)
        data->haveDups = true;

    return res;
}
```