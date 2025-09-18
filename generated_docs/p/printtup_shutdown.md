# printtup_shutdown

## Location
[src/backend/access/common/printtup.c:389-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L389-L412)

## Overview
The printtup_shutdown function performs cleanup operations for a printtup DestReceiver, freeing allocated memory and resources.

## Definition


## Detailed Description
The printtup_shutdown function is responsible for cleaning up resources associated with a DR_printtup structure when it's no longer needed. This function is typically called when a query execution completes or when the DestReceiver is being destroyed.

The function performs the following cleanup operations:
1. Frees the attribute information array (myinfo) if it exists
2. Sets the attribute info pointer to NULL
3. Frees the string buffer data if it exists
4. Sets the buffer data pointer to NULL
5. Deletes the temporary memory context if it exists
6. Sets the temporary context pointer to NULL

This ensures that all dynamically allocated memory associated with the printtup operation is properly released, preventing memory leaks.

## Parameters / Member Variables
- : DestReceiver pointer that contains the DR_printtup state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](pfree.md): PostgreSQL memory deallocation function
  - [MemoryContextDelete](../M/MemoryContextDelete.md): Deletes a memory context and all its contents
- Called from (representative examples):
  - [printtup_create_DR](printtup_create_DR.md): Sets this function as the shutdown handler for the DestReceiver

## Notes and Other Information
- The function is marked as static, indicating it's only used within the printtup.c file
- All pointer fields are explicitly set to NULL after freeing to prevent dangling pointer issues
- The function safely handles cases where pointers might already be NULL by checking before freeing
- This is a cleanup function that should be called exactly once per DR_printtup instance