# RemoveProcFromArray

## Location
[src/backend/storage/lmgr/proc.c:828-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L828-L838)

## Overview
Removes the current process from the shared ProcArray, typically called during process cleanup or termination.

## Definition

```c
static void
RemoveProcFromArray(int code, Datum arg)
```
## Detailed Description
RemoveProcFromArray is a static cleanup function that removes the current process (MyProc) from the shared ProcArray data structure. This function serves as an exit callback that ensures proper cleanup when a backend process terminates.

The function is designed to be registered as an exit callback and is called automatically during process shutdown. It delegates the actual removal logic to ProcArrayRemove, passing the current process and InvalidTransactionId to indicate that the process is being removed during shutdown rather than for transaction-specific reasons.

This function is part of PostgreSQL's process lifecycle management, ensuring that terminated processes are properly removed from shared data structures to prevent resource leaks and maintain system consistency.

## Parameters / Member Variables
- : Exit code (unused in this function but required by exit callback interface)
- : Datum argument (unused in this function but required by exit callback interface)

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayRemove](../P/ProcArrayRemove.md)
  - InvalidTransactionId (constant)
- Called from (representative examples):
  - [InitProcessPhase2](../I/InitProcessPhase2.md) (registered as exit callback)

## Notes and Other Information
- This is a static function, only accessible within proc.c
- Function parameters follow the standard exit callback signature but are not used in the implementation
- Uses InvalidTransactionId to indicate process removal during shutdown rather than transaction end
- The Assert(MyProc != NULL) ensures the function is only called for properly initialized processes
- Registered as an exit callback during process initialization to ensure automatic cleanup

## Simplified Source

```c
// Simplified version of RemoveProcFromArray
static void
RemoveProcFromArray(int code, Datum arg)
{
    // Ensure process is properly initialized
    Assert(MyProc != NULL);

    // Remove current process from shared ProcArray during shutdown
    ProcArrayRemove(MyProc, InvalidTransactionId);
}
```

Key simplifications made:
- Function is already very simple, minimal changes needed
- Added explanatory comments for the assertion and main operation
- Preserved the core logic: validation and delegation to ProcArrayRemove
- Maintained the exit callback signature parameters (code, arg) even though unused