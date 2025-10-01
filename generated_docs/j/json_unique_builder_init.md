# json_unique_builder_init

## Location
[src/backend/utils/adt/json.c:941-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L941-L948)

## Overview
The  function initializes a JSON unique builder state structure for tracking key uniqueness and managing skipped keys during JSON object construction.

## Definition

```c
static void
json_unique_builder_init(JsonUniqueBuilderState *cxt)
```
## Detailed Description
This function initializes a  structure that manages both key uniqueness checking and the handling of skipped keys (keys with NULL values) during JSON object building operations. It delegates the hash table initialization to , sets the memory context for key management, and initializes the skipped keys string buffer to NULL.

The function sets up the infrastructure needed for building JSON objects while ensuring key uniqueness and properly handling NULL value scenarios. The memory context reference allows proper cleanup and management of temporarily stored key information.

## Parameters / Member Variables
- : Pointer to a  structure to initialize

## Dependencies
- Functions called/Symbols referenced:
  - : Context structure for JSON building with uniqueness checking
  - : Initializes the embedded hash table for key uniqueness checking
  - : Global variable for the current memory allocation context

- Called from (representative examples):
  - : Used in JSON object aggregation operations
  - : Used in JSON object building functions

## Notes and Other Information
- This is a static function internal to the JSON aggregate and building implementation
- The function initializes three key components: uniqueness checking hash table, memory context, and skipped keys buffer
- The skipped keys mechanism handles cases where JSON object construction encounters NULL values that should be omitted from the final object
- Part of PostgreSQL's JSON object construction infrastructure that ensures proper key handling and JSON standard compliance
- The memory context reference enables proper resource management during JSON building operations
- Used in both aggregation scenarios (multiple input rows) and direct building scenarios (function calls)

## Simplified Source

```c
static void json_unique_builder_init(JsonUniqueBuilderState *cxt) {
    // Initialize hash table for key uniqueness checking
    json_unique_check_init(&cxt->check);

    // Set memory context for key management
    cxt->mcxt = CurrentMemoryContext;

    // Initialize skipped keys buffer to NULL (lazy initialization)
    cxt->skipped_keys.data = NULL;
}
```