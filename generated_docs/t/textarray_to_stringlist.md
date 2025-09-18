# textarray_to_stringlist

## Location
src/backend/catalog/pg_subscription.c: 210 - 235

## Overview
Converts a PostgreSQL text array (ArrayType) into a List of String nodes, with memory allocated in the current memory context.

## Definition
```c
static List *textarray_to_stringlist(ArrayType *textarray)
```

## Detailed Description
textarray_to_stringlist is a static utility function that transforms a PostgreSQL ArrayType containing text elements into a PostgreSQL List containing String nodes. It uses deconstruct_array_builtin to extract individual text datums from the array, then converts each datum to a C string and wraps it in a String node before appending to the result list. The function handles empty arrays by returning NIL and allocates all resulting strings using PostgreSQL's memory management system.

## Parameters / Member Variables
- `textarray`: Pointer to the ArrayType structure containing text elements to convert

## Dependencies
- Functions called/Symbols referenced:
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md) (extract array elements into datums)
  - TextDatumGetCString (convert text datum to C string)
  - [makeString](../m/makeString.md) (create String node from C string)
  - lappend (append element to list)
- Called from (representative examples):
  - [GetSubscription](../G/GetSubscription.md) (convert subscription publications array to string list)

## Notes and Other Information
- Static function, only accessible within pg_subscription.c
- Returns NIL for empty arrays, following PostgreSQL List conventions
- All string memory is allocated in the current memory context using palloc
- Uses TEXTOID as the expected array element type for deconstruct_array_builtin
- Part of PostgreSQL's subscription management system, specifically used for handling publication name arrays
- The resulting List contains String nodes, not raw C strings
- Memory allocated by this function should be managed by the calling context
- Essential for converting stored array data into usable list structures for subscription processing