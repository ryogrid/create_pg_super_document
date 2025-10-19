# init_gin_entries

## Location
[src/backend/utils/adt/jsonb_gin.c:163-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L163-L171)

## Overview
Initializes a GinEntries structure with a specified pre-allocation size for storing GIN index entries used in JSONB indexing operations.

## Definition

```c
static void
init_gin_entries(GinEntries *entries, int preallocated)
```
## Detailed Description
This static function initializes a GinEntries buffer structure that is used to collect and manage GIN (Generalized Inverted Index) entries during JSONB extraction operations. The function sets up the initial state of the buffer, optionally pre-allocating memory for a specified number of Datum entries to improve performance by reducing the need for dynamic memory allocation during entry collection.

The function is part of PostgreSQL's JSONB GIN indexing infrastructure, which allows efficient indexing and querying of JSONB data by extracting individual keys, values, and paths as separate index entries.

## Parameters / Member Variables
- `*entries`: Pointer to the GinEntries structure to be initialized
- `preallocated`: Number of Datum entries to pre-allocate in the buffer; if 0, no initial allocation is performed
## Dependencies
- Functions called/Symbols referenced:
  - [GinEntries](../G/GinEntries.md) (struct type)
  - [palloc](../p/palloc.md) (memory allocation function)
- Called from (representative examples):
  - [gin_extract_jsonb](../g/gin_extract_jsonb.md) (at src/backend/utils/adt/jsonb_gin.c:247)
  - [gin_extract_jsonb_path](../g/gin_extract_jsonb_path.md) (at src/backend/utils/adt/jsonb_gin.c:1110)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonb_gin.c file
- The pre-allocation optimization helps reduce memory fragmentation and allocation overhead when the approximate number of entries is known in advance
- The GinEntries structure manages a dynamic buffer of Datum values used in GIN index operations
- Memory is allocated using PostgreSQL's palloc() function, which integrates with the database's memory context system

## Simplified Source

```c
static void
init_gin_entries(GinEntries *entries, int preallocated)
{
    // Set up initial buffer state
    entries->allocated = preallocated;
    entries->count = 0;

    // Pre-allocate memory if requested for performance
    entries->buf = preallocated ? palloc(sizeof(Datum) * preallocated) : NULL;
}
```