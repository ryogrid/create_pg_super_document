# TestDSMRegistryStruct

## Location
[src/test/modules/test_dsm_registry/test_dsm_registry.c:21-25](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_dsm_registry/test_dsm_registry.c#L21-L25)

## Overview
TestDSMRegistryStruct is a simple test structure designed to demonstrate and test the dynamic shared memory (DSM) registry functionality in PostgreSQL's test module.

## Definition

```c
typedef struct TestDSMRegistryStruct
{
	int			val;
	LWLock		lck;
} TestDSMRegistryStruct;
```
## Detailed Description
TestDSMRegistryStruct is a minimal structure used exclusively within PostgreSQL's test_dsm_registry test module to validate the dynamic shared memory registry system. The structure contains just two members: an integer value for storing test data and a lightweight lock (LWLock) for coordinating concurrent access to the shared memory segment.

This structure is allocated in dynamic shared memory via the DSM registry mechanism and provides a thread-safe way to store and retrieve integer values across multiple PostgreSQL processes. The structure is initialized once when first created and subsequently accessed by multiple processes that attach to the same named DSM segment.

The test module uses this structure to demonstrate proper initialization, attachment, and synchronized access patterns for DSM registry-managed shared memory segments.

## Parameters / Member Variables
- : Integer value used for testing shared memory operations - stores and retrieves test data across processes
- : LWLock (lightweight lock) that provides thread-safe access coordination for the shared memory segment

## Dependencies
- Functions called/Symbols referenced:
  - LWLock (from storage/lwlock.h)

- Called from (representative examples):
  - tdr_init_shmem (initialization function that sets up the structure in shared memory)
  - tdr_attach_shmem (attachment function that retrieves the structure from DSM registry)

## Notes and Other Information
- This structure is exclusively used for testing purposes within src/test/modules/test_dsm_registry/
- The structure demonstrates proper patterns for DSM registry usage: initialization with tdr_init_shmem, attachment via GetNamedDSMSegment, and synchronized access using LWLocks
- The lock is initialized with LWLockInitialize() and registered with LWLockRegisterTranche() for proper tranche management
- Access to the val member is always protected by acquiring the LWLock in either LW_EXCLUSIVE (for writes) or LW_SHARED (for reads) mode
- The structure serves as a minimal example of how to properly structure data for DSM registry-managed shared memory segments