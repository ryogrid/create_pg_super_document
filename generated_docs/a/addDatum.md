# addDatum

## Location
src/backend/access/gin/ginfast.c: 685 - 708

## Overview
Adds a datum and its associated null category to a KeyArray, automatically resizing the array if needed.

## Definition
```c
static void addDatum(KeyArray *keys, Datum datum, GinNullCategory category)
```

## Detailed Description
This function adds a new datum-category pair to an existing KeyArray structure. It includes automatic memory management by doubling the array size when the current capacity is exceeded. The function maintains parallel arrays for datum values and their corresponding null categories, ensuring both arrays remain synchronized. This dynamic resizing capability makes it suitable for scenarios where the number of keys to be processed is not known in advance.

## Parameters / Member Variables
- `keys`: Pointer to the KeyArray structure to add the datum to
- `datum`: The Datum value to be added to the array
- `category`: The GinNullCategory classification for this datum (e.g., normal value, null, empty)

## Dependencies
- Functions called/Symbols referenced:
  - repalloc_array (for resizing memory arrays)
  - KeyArray (structure type)
  - GinNullCategory (enumeration type)
- Called from (representative examples):
  - processPendingPage (at src/backend/access/gin/ginfast.c:757)

## Notes and Other Information
- This is a static function, accessible only within the ginfast.c file
- Implements a dynamic array growth strategy by doubling capacity when full
- Uses repalloc_array for memory reallocation, which is PostgreSQL's reallocation function
- Maintains synchronization between the keys and categories arrays
- The growth factor of 2x provides a good balance between memory usage and reallocation frequency
- Increments nvalues after successfully adding the datum to track the current array size