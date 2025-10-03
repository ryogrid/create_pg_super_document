# SerializeGUCState

## Location
[src/backend/utils/misc/guc.c:6109-6141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6109-L6141)

## Overview
Serializes the complete GUC (Grand Unified Configuration) state by dumping all non-default configuration variables into a memory buffer for transfer to parallel workers or other processes.

## Definition

```c
struct config_generic *gconf = dlist_container(struct config_generic,
													   nondef_link, iter.cur);
```
## Detailed Description
The `SerializeGUCState` function is responsible for creating a complete serialized snapshot of PostgreSQL's current configuration state. It iterates through all GUC variables that have been modified from their default values (stored in the `guc_nondef_list`) and serializes each one using the `serialize_variable` function.

The function reserves space at the beginning of the output buffer to store the actual size of the serialized data. It then processes each non-default GUC variable in the list, calling `serialize_variable` to convert each one into the serialized format. After all variables have been processed, the actual size of the serialized data is calculated and stored at the beginning of the buffer.

This serialization mechanism is primarily used in parallel query processing to ensure that worker processes inherit the same configuration state as the leader process.

## Parameters / Member Variables
- `maxsize`: Maximum size of the destination buffer in bytes
- `start_address`: Pointer to the beginning of the destination buffer where serialized data will be written

## Dependencies
- Functions called/Symbols referenced:
  - [serialize_variable](../s/serialize_variable.md)
  - dlist_foreach
  - dlist_container
  - memcpy
- Data structures:
  - [config_generic](../c/config_generic.md)
  - [dlist_iter](../d/dlist_iter.md)
- Global variables:
  - guc_nondef_list
- Called from:
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Only processes GUC variables with source other than PGC_S_DEFAULT to avoid serializing unchanged configuration
- The actual size is stored at the beginning of the buffer without assuming any alignment requirements
- Used primarily in PostgreSQL's parallel processing infrastructure to synchronize configuration between leader and worker processes
- The function assumes the provided buffer is large enough; callers should use `EstimateGUCStateSpace` to determine required buffer size
- Part of PostgreSQL's mechanism for maintaining configuration consistency across process boundaries in parallel operations

## Simplified Source

```c
void SerializeGUCState(Size maxsize, char *start_address) {
    char *curptr;
    Size actual_size;
    Size bytes_left;
    dlist_iter iter;

    // Reserve space for actual size at beginning
    curptr = start_address + sizeof(actual_size);
    bytes_left = maxsize - sizeof(actual_size);

    // Serialize all non-default GUC variables
    dlist_foreach(iter, &guc_nondef_list) {
        struct config_generic *gconf = dlist_container(struct config_generic,
                                                       nondef_link, iter.cur);
        serialize_variable(&curptr, &bytes_left, gconf);
    }

    // Store actual size at the beginning
    actual_size = maxsize - bytes_left - sizeof(actual_size);
    memcpy(start_address, &actual_size, sizeof(actual_size));
}
```