# RestoreClientConnectionInfo

## Location
src/backend/utils/init/miscinit.c: 1130 - 1169

## Overview
Restores MyClientConnectionInfo from its serialized binary representation, typically used by parallel worker processes to inherit client connection information.

## Definition
```c
void RestoreClientConnectionInfo(char *conninfo)
```

## Detailed Description
This function deserializes client connection information from a binary buffer that was previously created by `SerializeClientConnectionInfo()`. It reconstructs the global `MyClientConnectionInfo` structure, including both the fixed-size authentication method and variable-length authentication identifier string. The authentication ID string is allocated in TopMemoryContext to ensure it persists for the lifetime of the process. This is primarily used by parallel worker processes to inherit the client connection context from the main backend process.

## Parameters / Member Variables
- `conninfo`: Pointer to the serialized client connection information buffer

## Dependencies
- Functions called/Symbols referenced:
  - `[SerializedClientConnectionInfo](../S/SerializedClientConnectionInfo.md)` - Structure type for the serialized format
  - `[MemoryContextStrdup](../M/MemoryContextStrdup.md)` - Memory context-aware string duplication function
  - `TopMemoryContext` - Long-lived memory context for process lifetime allocations
  - `MyClientConnectionInfo` - Global variable to store restored connection info
  - `memcpy` - Standard memory copy function
- Called from (representative examples):
  - `[ParallelWorkerMain](../P/ParallelWorkerMain.md)` - When parallel workers initialize client connection context
  - `INIT_PG_OVERRIDE_ROLE_LOGIN` - In role login override scenarios

## Notes and Other Information
- Counterpart to `SerializeClientConnectionInfo()` - these functions must be kept in sync
- The authentication ID is duplicated into TopMemoryContext for long-term storage
- Handles null authentication ID gracefully (when authn_id_len < 0)
- Assumes the serialized buffer was created by the corresponding serialize function
- Critical for maintaining proper authentication context in parallel query execution
- The restored authentication ID persists until process termination due to TopMemoryContext allocation