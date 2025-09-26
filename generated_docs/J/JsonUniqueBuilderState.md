# JsonUniqueBuilderState

## Location
[src/backend/utils/adt/json.c:67-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L67-L72)

## Overview
JsonUniqueBuilderState is a context structure that manages key uniqueness checking and NULL value handling during JSON object construction and building operations.

## Definition

```c
typedef struct JsonUniqueBuilderState
{
	JsonUniqueCheckState check; /* unique check */
	StringInfoData skipped_keys;	/* skipped keys with NULL values */
	MemoryContext mcxt;			/* context for saving skipped keys */
} JsonUniqueBuilderState;
```
## Detailed Description
JsonUniqueBuilderState provides specialized support for JSON object construction with integrated key uniqueness validation and NULL value management. Unlike the parsing state which processes existing JSON text, this structure supports the dynamic construction of JSON objects from individual key-value pairs. It extends the basic uniqueness checking capability with additional features for handling NULL values and managing memory for temporarily stored information.

The structure is optimized for JSON building scenarios where keys with NULL values might be excluded from the final JSON object, requiring temporary storage and special handling. The integrated memory context ensures proper cleanup of temporary data structures used during the building process.

## Parameters / Member Variables
- : Hash table state (JsonUniqueCheckState) for fast duplicate key detection during JSON object construction
- : StringInfoData buffer for temporarily storing keys that have NULL values and may be excluded from the final JSON object
- : Memory context for managing allocations related to skipped key storage and other temporary data structures

## Dependencies
- Functions called/Symbols referenced:
  - JsonUniqueCheckState (hash table for key uniqueness checking)
  - StringInfoData (PostgreSQL's extensible string buffer)
  - MemoryContext (PostgreSQL's memory management system)
- Called from (representative examples):
  - json_unique_builder_init (initialization of builder state)
  - json_unique_builder_get_throwawaybuf (retrieving temporary buffer)
  - json_build_object_worker (main JSON object building function)
  - JsonAggState (as a member in JSON aggregation contexts)

## Notes and Other Information
- Specialized for JSON construction scenarios rather than parsing of existing JSON text
- Handles the complex case where keys with NULL values may need to be excluded from the final JSON object
- The skipped_keys buffer allows for deferred decisions about which keys to include in the final JSON
- Memory context management ensures proper cleanup of temporary allocations during building
- Used in JSON aggregation operations where multiple key-value pairs are combined into single JSON objects
- Provides both uniqueness validation and flexible NULL value handling in a single structure
- Integrates with PostgreSQL's memory management system for efficient resource utilization
- Essential component for JSON building functions that need to maintain key uniqueness while handling edge cases