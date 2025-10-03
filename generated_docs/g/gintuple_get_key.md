# gintuple_get_key

## Location
[src/backend/access/gin/ginutil.c:259-299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L259-L299)

## Overview
Extracts the stored datum (key value) and its null category from a GIN index tuple, handling both single-column and multi-column index formats.

## Definition

```c
Datum
gintuple_get_key(GinState *ginstate, IndexTuple tuple,
				 GinNullCategory *category)
```
## Detailed Description
This function extracts the actual key value stored in a GIN index tuple and determines its null category. The extraction process differs between single-column and multi-column indexes due to their different internal storage formats.

For single-column indexes, the key is stored as the first (and only) attribute of the tuple. For multi-column indexes, the first attribute contains the column number, and the actual key value is stored as the second attribute. The function uses the appropriate tuple descriptor based on which column the tuple represents to ensure correct data type interpretation.

The function also handles null values by calling  to determine the specific type of null representation used in GIN indexes, or sets the category to  for non-null values.

## Parameters / Member Variables
- `*ginstate`: Pointer to the GinState structure containing index metadata and tuple descriptors
- `tuple`: The IndexTuple from which to extract the key value
- `*category`: Output parameter that receives the null category classification
## Dependencies
- Functions called/Symbols referenced:
  -  (extract attribute from tuple)
  -  (get column number for multi-column indexes)
  -  (determine null category for null values)
  -  (constant for first attribute position)
  -  (get next offset number)
  -  (constant for normal key category)

- Called from:
  -  (src/backend/access/gin/ginentrypage.c:255)
  -  (src/backend/access/gin/ginentrypage.c:312)
  -  (src/backend/access/gin/ginentrypage.c:383)
  -  (src/backend/access/gin/ginfast.c:756)
  -  (src/backend/access/gin/ginget.c:170, 282)
  -  (src/backend/access/gin/ginget.c:1563)
  -  (src/backend/access/gin/ginget.c:1690)
  -  (src/backend/access/gin/gininsert.c:64)
  -  (src/backend/access/gin/ginvacuum.c:543)

## Notes and Other Information
- For single-column indexes, extracts the key from the first attribute using the original tuple descriptor
- For multi-column indexes, first determines the column number, then extracts the key from the second attribute using the appropriate column-specific tuple descriptor
- The function properly handles the different tuple formats used by single-column vs multi-column GIN indexes
- Null values are handled specially through the GIN null category system, which allows GIN to distinguish between different types of null/missing values
- The  parameter is always set, either to a specific null category or to  for regular values
- This is a fundamental utility function used throughout GIN operations for key extraction and comparison
- The function ensures type safety by using the correct tuple descriptor for the specific column being processed

## Simplified Source

```c
// Simplified version of gintuple_get_key
Datum
gintuple_get_key(GinState *ginstate, IndexTuple tuple,
                 GinNullCategory *category)
{
    Datum res;
    bool isnull;

    if (ginstate->oneCol)
    {
        // Single column: key is first attribute
        res = index_getattr(tuple, FirstOffsetNumber, ginstate->origTupdesc,
                           &isnull);
    }
    else
    {
        // Multi-column: get column number, then extract key from second attribute
        OffsetNumber colN = gintuple_get_attrnum(ginstate, tuple);

        res = index_getattr(tuple, OffsetNumberNext(FirstOffsetNumber),
                           ginstate->tupdesc[colN - 1],
                           &isnull);
    }

    // Set appropriate null category
    if (isnull)
        *category = GinGetNullCategory(tuple, ginstate);
    else
        *category = GIN_CAT_NORM_KEY;

    return res;
}
```