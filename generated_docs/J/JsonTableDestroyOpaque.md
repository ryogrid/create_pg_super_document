# JsonTableDestroyOpaque

## Location
[src/backend/utils/adt/jsonpath_exec.c:4176-4192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4176-L4192)

## Overview
Cleans up and resets the opaque context used for JSON_TABLE processing by invalidating the execution context and clearing the state.

## Definition
```c
static void JsonTableDestroyOpaque(TableFuncScanState *state)
```

## Detailed Description
JsonTableDestroyOpaque is a static cleanup function that properly tears down the JSON_TABLE execution context. It performs the following operations:

1. Retrieves the JsonTableExecContext from the TableFuncScanState->opaque field using GetJsonTableExecContext
2. Invalidates the context by setting the magic number to 0, making it unusable for future operations
3. Clears the state->opaque pointer by setting it to NULL

This function serves as the counterpart to JsonTableInitOpaque, ensuring proper cleanup of resources and preventing accidental reuse of invalidated contexts.

## Parameters / Member Variables
- `state`: TableFuncScanState pointer containing the scan state with the opaque context to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [GetJsonTableExecContext](../G/GetJsonTableExecContext.md) (context retrieval and validation)
  - [JsonTableExecContext](JsonTableExecContext.md) (struct type)
- Called from (representative examples):
  - Table function scan cleanup routines
  - Error handling paths

## Notes and Other Information
- This is a static function within jsonpath_exec.c, indicating it's internal to JSON path execution
- The function uses GetJsonTableExecContext which likely validates the magic number before returning the context
- Setting the magic number to 0 is a defensive programming practice to detect use-after-free scenarios
- The function is simple but critical for proper resource management in JSON_TABLE operations
- After this function is called, any attempt to use the context should result in an error due to the invalidated magic number