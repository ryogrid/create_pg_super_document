# RestoreClientConnectionInfo

## Location
[src/backend/utils/init/miscinit.c:1130-1169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1130-L1169)

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
  - [SerializedClientConnectionInfo](../S/SerializedClientConnectionInfo.md) - Structure type for the serialized format
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) - Memory context-aware string duplication function
  - `TopMemoryContext` - Long-lived memory context for process lifetime allocations
  - `MyClientConnectionInfo` - Global variable to store restored connection info
  - `memcpy` - Standard memory copy function
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) - When parallel workers initialize client connection context
  - `INIT_PG_OVERRIDE_ROLE_LOGIN` - In role login override scenarios

## Notes and Other Information
- Counterpart to `SerializeClientConnectionInfo()` - these functions must be kept in sync
- The authentication ID is duplicated into TopMemoryContext for long-term storage
- Handles null authentication ID gracefully (when authn_id_len < 0)
- Assumes the serialized buffer was created by the corresponding serialize function
- Critical for maintaining proper authentication context in parallel query execution
- The restored authentication ID persists until process termination due to TopMemoryContext allocation

## Simplified Source

```c
void RestoreClientConnectionInfo(char *conninfo)
{
    SerializedClientConnectionInfo serialized;

    // Deserialize the fixed-size portion
    memcpy(&serialized, conninfo, sizeof(serialized));

    // Restore authentication method
    MyClientConnectionInfo.authn_id = NULL;
    MyClientConnectionInfo.auth_method = serialized.auth_method;

    // Restore authentication ID if present
    if (serialized.authn_id_len >= 0) {
        char *authn_id = conninfo + sizeof(serialized);
        MyClientConnectionInfo.authn_id = MemoryContextStrdup(TopMemoryContext, authn_id);
    }
}
```