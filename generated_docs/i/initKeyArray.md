# initKeyArray

## Location
[src/backend/access/gin/ginfast.c:675-684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginfast.c#L675-L684)

## Overview
Initializes an empty KeyArray structure for storing GIN index keys and their associated null categories.

## Definition

```c
static void
initKeyArray(KeyArray *keys, int32 maxvalues)
```
## Detailed Description
This function initializes a KeyArray structure by allocating memory for storing Datum values and their corresponding GinNullCategory classifications. It sets up the initial state of the KeyArray with zero values and establishes the maximum capacity. This is typically used in GIN index operations to prepare a container for collecting and processing index keys before insertion or cleanup operations.

## Parameters / Member Variables
- `*keys`: Pointer to the KeyArray structure to be initialized
- `maxvalues`: Maximum number of values that can be stored in this KeyArray
## Dependencies
- Functions called/Symbols referenced:
  - palloc_array (for allocating memory arrays)
  - [KeyArray](../K/KeyArray.md) (structure type)
  - GinNullCategory (enumeration type)
- Called from (representative examples):
  - [ginInsertCleanup](../g/ginInsertCleanup.md) (at src/backend/access/gin/ginfast.c:868)
  - [ginInsertCleanup](../g/ginInsertCleanup.md) (at src/backend/access/gin/ginfast.c:993)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the ginfast.c file
- The function allocates two parallel arrays: one for Datum values and one for null categories
- Initial nvalues is set to 0, indicating an empty array
- Memory allocation uses palloc_array which is PostgreSQL's memory allocation function
- The KeyArray structure is commonly used in GIN index fast insertion paths

## Simplified Source

```c
static void initKeyArray(KeyArray *keys, int32 maxvalues) {
    // Allocate parallel arrays for keys and their null categories
    keys->keys = palloc_array(Datum, maxvalues);
    keys->categories = palloc_array(GinNullCategory, maxvalues);

    // Initialize state
    keys->nvalues = 0;          // Start with empty array
    keys->maxvalues = maxvalues; // Set capacity limit
}
```