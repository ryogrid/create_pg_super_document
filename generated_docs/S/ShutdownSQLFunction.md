# ShutdownSQLFunction

## Location
[src/backend/executor/functions.c:1488-1533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L1488-L1533)

## Overview
A callback function used to shut down SQL functions that return sets before they have been run to completion, ensuring proper cleanup of execution states and resources.

## Definition
```c
static void ShutdownSQLFunction(Datum arg)
```

## Detailed Description
ShutdownSQLFunction is a critical cleanup callback function in PostgreSQL's SQL function execution framework. It is specifically designed to handle the shutdown of set-returning SQL functions that may be terminated before completing their full execution cycle. The function performs comprehensive cleanup operations including:

1. **Execution State Management**: Iterates through all execution states in the function cache and properly shuts down any that are still running (status F_EXEC_RUN).

2. **Snapshot Management**: For non-readonly functions, it manages active snapshots by pushing and popping them during the shutdown process to maintain consistency.

3. **Resource Cleanup**: Releases the tuplestore if one exists and resets execution states to F_EXEC_START for potential reuse.

4. **Callback Deregistration**: Marks the shutdown callback as no longer registered.

## Parameters / Member Variables
- `arg`: A Datum containing a pointer to the SQLFunctionCachePtr that needs to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - [postquel_end](../p/postquel_end.md)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [tuplestore_end](../t/tuplestore_end.md)
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md) (during function execution setup and cleanup)

## Notes and Other Information
- This is a static function, only accessible within functions.c
- Essential for preventing resource leaks when set-returning functions are prematurely terminated
- Handles both readonly and non-readonly function types with appropriate snapshot management
- Works in conjunction with the executor's callback registration system
- The function resets execution states to F_EXEC_START, allowing for potential reuse of the execution context