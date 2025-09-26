# get_val_in_shmem

## Location
src/test/modules/test_dsm_registry/test_dsm_registry.c: 65 - 76

## Overview
A PostgreSQL SQL-callable function that retrieves an integer value from the test DSM registry's shared memory with proper locking for safe concurrent access.

## Definition
```c
Datum get_val_in_shmem(PG_FUNCTION_ARGS)
```

## Detailed Description
The `get_val_in_shmem` function is a PostgreSQL SQL-callable function (exposed via PG_FUNCTION_INFO_V1) that allows reading the integer value stored in the shared memory managed by the test_dsm_registry module. The function first ensures attachment to the DSM segment by calling `tdr_attach_shmem`, then acquires a shared lock on the shared memory structure to safely read the value field. This provides thread-safe read access to the shared integer value across multiple PostgreSQL processes.

The function uses a shared lock (LW_SHARED) which allows multiple concurrent readers while preventing writers from modifying the value during the read operation. The retrieved value is returned as a PostgreSQL Datum to the SQL caller.

## Parameters / Member Variables
This function takes no SQL parameters and returns the integer value stored in shared memory.

## Dependencies
- Functions called/Symbols referenced:
  - `tdr_attach_shmem` - Ensures DSM segment is attached
  - `LWLockAcquire` - Acquires shared lock for safe reading
  - `LWLockRelease` - Releases the lock after operation
  - `PG_RETURN_INT32` - Returns the integer value to SQL caller
  - `LW_SHARED` - Shared lock mode constant for concurrent reads
- Called from (representative examples):
  - SQL queries that invoke `get_val_in_shmem()`

## Notes and Other Information
- This is a SQL-callable PostgreSQL function, callable from SQL as `SELECT get_val_in_shmem();`
- Part of the test_dsm_registry test module for testing DSM functionality
- Uses shared locking (LW_SHARED) to allow multiple concurrent readers while blocking writers
- Returns the current integer value stored in the shared memory structure
- Automatically handles DSM segment attachment on each call, making it safe to call even if the segment isn't currently attached
- The global `tdr_state` variable is updated by the attach function and then used to access the shared memory
- Complements the `set_val_in_shmem` function to provide a complete read/write interface for the shared value