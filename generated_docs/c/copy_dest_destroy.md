# copy_dest_destroy

## Location
[src/backend/commands/copyto.c:1263-1271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L1263-L1271)

## Overview
Releases memory allocated for the COPY destination receiver when it's no longer needed.

## Definition

```c
static void
copy_dest_destroy(DestReceiver *self)
```
## Detailed Description
This function serves as the cleanup callback for the COPY destination receiver in PostgreSQL's executor framework. It implements the DestReceiver interface requirement for resource cleanup by freeing the memory allocated for the destination receiver structure. This function is called when the COPY operation is complete and the destination receiver is no longer needed. The function performs a simple memory deallocation using pfree, PostgreSQL's memory management function. Note that the actual COPY state (CopyToState) cleanup is handled separately in the main COPY processing flow, so this function only needs to clean up the destination receiver wrapper structure itself.

## Parameters / Member Variables
- : Pointer to the DestReceiver structure representing the COPY destination receiver to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  -  (interface structure)
  -  (PostgreSQL memory deallocation function)
- Called from (representative examples):
  -  (during receiver setup as callback assignment)

## Notes and Other Information
- This is a callback function that gets assigned to the DestReceiver's rDestroy field during COPY destination receiver initialization
- The function only frees the destination receiver structure itself, not the underlying COPY state which is managed separately
- Part of PostgreSQL's memory management and cleanup framework for destination receivers
- Uses pfree rather than standard free() as part of PostgreSQL's palloc/pfree memory management system
- The separation of concerns means that CopyToState cleanup happens in the main COPY workflow, while this function handles only the receiver wrapper cleanup

## Simplified Source

```c
// Simplified version of copy_dest_destroy
static void
copy_dest_destroy(DestReceiver *self)
{
    // Release the destination receiver memory
    pfree(self);
}
```

Key simplifications made:
- Function is already minimal - only performs memory cleanup
- No error handling needed as pfree handles null pointers safely
- Single responsibility: free the DestReceiver structure
- Uses PostgreSQL's memory management (pfree vs standard free)