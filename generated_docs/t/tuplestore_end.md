# tuplestore_end

## Location
[src/backend/utils/sort/tuplestore.c:453-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L453-L472)

## Overview
Releases resources and cleans up a tuplestore state structure, freeing all allocated memory and closing any associated files.

## Definition

```c
void
tuplestore_end(Tuplestorestate *state)
```
## Detailed Description
The  function performs cleanup operations for a tuplestore by:
1. Closing the temporary file (if any) using 
2. Freeing all stored tuples in the memory array from  to 
3. Freeing the  array itself
4. Freeing the read pointers array ()
5. Freeing the main state structure

This function should be called when the tuplestore is no longer needed to prevent memory leaks and ensure proper resource cleanup.

## Parameters / Member Variables
- `*state`: Pointer to the  structure to be cleaned up and freed
## Dependencies
- Functions called/Symbols referenced:
  -  - closes the temporary file if one exists
  -  - PostgreSQL memory deallocation function
- Called from (representative examples):
  -  (trigger.c:5229, 5240, 5244, 5248, 5252)
  -  (execSRF.c:553)
  -  (nodeMaterial.c:246)
  -  (nodeRecursiveunion.c:275, 276)
  -  (portalmem.c:585)

## Notes and Other Information
- This is the cleanup counterpart to 
- Must be called exactly once for each tuplestore created
- After calling this function, the state pointer becomes invalid and should not be used
- The function handles NULL pointers gracefully (checks before freeing)
- Used extensively in executor nodes and various PostgreSQL subsystems that utilize tuplestores for temporary storage

## Simplified Source

```c
// Simplified version of tuplestore_end
void
tuplestore_end(Tuplestorestate *state)
{
    // Close temporary file if it exists
    if (state->myfile)
        BufFileClose(state->myfile);

    // Free all stored tuples and the tuple array
    if (state->memtuples)
    {
        // Free individual tuples from deleted index to current count
        for (int i = state->memtupdeleted; i < state->memtupcount; i++)
            pfree(state->memtuples[i]);

        // Free the tuple pointer array
        pfree(state->memtuples);
    }

    // Free read pointers array
    pfree(state->readptrs);

    // Free the main state structure
    pfree(state);
}
```

Key simplifications made:
- Added explanatory comments for each cleanup phase
- Moved variable declaration inline in the for loop
- Preserved essential logic: close file, free tuples, free arrays, free state
- Maintained the proper cleanup order and NULL checking
- Clear separation of different resource types being freed