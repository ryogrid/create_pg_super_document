# KeyArray

## Location
src/backend/access/gin/ginfast.c: 44 - 50

## Overview
KeyArray is a structure used in the PostgreSQL GIN (Generalized Inverted Index) fast insert implementation to store an expandable array of key values and their associated null categories.

## Definition
```c
typedef struct KeyArray
{
    Datum           *keys;        /* expansible array */
    GinNullCategory *categories;  /* another expansible array */
    int32           nvalues;      /* current number of valid entries */
    int32           maxvalues;    /* allocated size of arrays */
} KeyArray;
```

## Detailed Description
KeyArray is a dynamic data structure defined in `src/backend/access/gin/ginfast.c` that manages parallel arrays of key values and their corresponding null categories. It is specifically designed for GIN index operations where keys need to be collected and processed efficiently. The structure automatically expands its storage capacity when needed, doubling the array size when the current capacity is exceeded.

The structure maintains two synchronized arrays: one for the actual key data (`keys`) and another for categorizing null values (`categories`). This design allows GIN indexes to properly handle various types of null values and empty queries while maintaining efficient access patterns.

## Parameters / Member Variables
- `keys`: A dynamically allocated array of `Datum` values representing the actual key data stored in the array
- `categories`: A parallel array of `GinNullCategory` values that categorize null-related states for each corresponding key
- `nvalues`: The current number of valid entries stored in both arrays, indicating how many elements are actually in use
- `maxvalues`: The allocated size of both arrays, representing the maximum number of elements that can be stored without reallocation

## Dependencies
- Functions called/Symbols referenced:
  - GinNullCategory
- Called from (representative examples):
  - initKeyArray
  - addDatum
  - processPendingPage
  - ginInsertCleanup

## Notes and Other Information
- The KeyArray structure is used internally in GIN fast insert operations to accumulate keys before processing them
- Memory management is handled automatically through `palloc_array` and `repalloc_array` functions
- The structure uses a doubling strategy for array growth to minimize reallocation overhead
- Both the keys and categories arrays are kept in sync, with each index position corresponding to the same logical entry
- This structure is part of the GIN access method implementation for handling pending list insertions efficiently