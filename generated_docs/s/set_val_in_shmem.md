# set_val_in_shmem

## Location
[src/test/modules/test_dsm_registry/test_dsm_registry.c:52-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_dsm_registry/test_dsm_registry.c#L52-L64)

## Overview
A PostgreSQL SQL-callable function that sets an integer value in the test DSM registry's shared memory with proper locking.

## Definition
```c
Datum set_val_in_shmem(PG_FUNCTION_ARGS)
```

## Detailed Description
The `set_val_in_shmem` function is a PostgreSQL SQL-callable function (exposed via PG_FUNCTION_INFO_V1) that allows setting an integer value in the shared memory managed by the test_dsm_registry module. The function first ensures attachment to the DSM segment by calling `tdr_attach_shmem`, then acquires an exclusive lock on the shared memory structure to safely update the value field. This provides thread-safe write access to the shared integer value across multiple PostgreSQL processes.

The function takes a single integer argument from SQL and stores it in the shared memory, replacing any previous value. The exclusive lock ensures that no other processes can read or write the value during the update operation.

## Parameters / Member Variables
- Function takes one SQL parameter: an integer value to store in shared memory (accessed via `PG_GETARG_INT32(0)`)

## Dependencies
- Functions called/Symbols referenced:
  - `[tdr_attach_shmem](../t/tdr_attach_shmem.md)` - Ensures DSM segment is attached
  - `[LWLockAcquire](../L/LWLockAcquire.md)` - Acquires exclusive lock for safe writing
  - `[LWLockRelease](../L/LWLockRelease.md)` - Releases the lock after operation
  - `PG_GETARG_INT32` - Retrieves the integer argument from SQL call
  - `PG_RETURN_VOID` - Returns void to SQL caller
  - `PG_FUNCTION_INFO_V1` - Declares function as SQL-callable
- Called from (representative examples):
  - SQL queries that invoke `set_val_in_shmem(integer_value)`

## Notes and Other Information
- This is a SQL-callable PostgreSQL function, callable from SQL as `SELECT set_val_in_shmem(42);`
- Part of the test_dsm_registry test module for testing DSM functionality
- Uses exclusive locking (LW_EXCLUSIVE) to prevent concurrent access during writes
- The function returns void to the SQL caller
- Automatically handles DSM segment attachment on each call, making it safe to call even if the segment isn't currently attached
- The global `tdr_state` variable is updated by the attach function and then used to access the shared memory

## Simplified Source

```c
Datum set_val_in_shmem(PG_FUNCTION_ARGS) {
    // Ensure we're attached to the shared memory segment
    tdr_attach_shmem();

    // Acquire exclusive lock for safe writing
    LWLockAcquire(&tdr_state->lck, LW_EXCLUSIVE);

    // Set the shared value from SQL parameter
    tdr_state->val = PG_GETARG_INT32(0);

    // Release the lock
    LWLockRelease(&tdr_state->lck);

    // Return void to SQL caller
    PG_RETURN_VOID();
}
```