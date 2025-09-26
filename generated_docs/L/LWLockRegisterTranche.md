# LWLockRegisterTranche

## Location
[src/backend/storage/lmgr/lwlock.c:630-671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L630-L671)

## Overview
Registers a dynamic tranche name in the lookup table of the current process for user-defined lightweight lock tranches.

## Definition
```c
void LWLockRegisterTranche(int tranche_id, const char *tranche_name)
```

## Detailed Description
This function manages the registration of user-defined lightweight lock tranches by storing their names in a process-local lookup table. It serves as a crucial component for wait event reporting, allowing PostgreSQL to display meaningful tranche names when processes are waiting on lightweight locks.

The function performs several key operations:
- Validates that the tranche_id is within the user-defined range (>= LWTRANCHE_FIRST_USER_DEFINED)
- Dynamically allocates or expands the LWLockTrancheNames array as needed
- Stores the tranche name pointer for later retrieval during wait event reporting

The implementation uses a power-of-2 allocation strategy to minimize memory reallocations when multiple tranches are registered.

## Parameters / Member Variables
- `tranche_id`: The unique identifier for the tranche, must be >= LWTRANCHE_FIRST_USER_DEFINED
- `tranche_name`: Pointer to the tranche name string, should be allocated in backend-lifetime context (shared memory, TopMemoryContext, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - LWTRANCHE_FIRST_USER_DEFINED (constant)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md) (memory allocation utility)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (memory allocation)
  - repalloc0_array (memory reallocation)
- Called from (representative examples):
  - [CreateLWLocks](../C/CreateLWLocks.md)
  - [test_dsa_basic](../t/test_dsa_basic.md)
  - [test_slru_shmem_startup](../t/test_slru_shmem_startup.md)
  - [tdr_attach_shmem](../t/tdr_attach_shmem.md)

## Notes and Other Information
- The tranche name will be user-visible as a wait event name, so it should follow PostgreSQL's naming conventions for wait events
- The function saves only a pointer to the tranche name, requiring the caller to ensure the name string remains valid for the process lifetime
- Only processes user-defined tranches; system-defined tranches are handled differently
- Thread-safe within a single process context but requires coordination for shared memory scenarios