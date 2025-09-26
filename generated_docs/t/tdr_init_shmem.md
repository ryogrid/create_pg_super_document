# tdr_init_shmem

## Location
src/test/modules/test_dsm_registry/test_dsm_registry.c: 30 - 38

## Overview
Initializes a shared memory structure for the test_dsm_registry module by setting up the lock mechanism and initializing the value field.

## Definition

```c
static void
tdr_init_shmem(void *ptr)
```
## Detailed Description
The `tdr_init_shmem` function is a static initialization callback used by PostgreSQL's Dynamic Shared Memory (DSM) registry system in the test module. It takes a raw pointer to shared memory and initializes it as a `TestDSMRegistryStruct`. The function performs two key operations: initializing a lightweight lock (`LWLock`) with a new tranche ID for synchronization, and setting the initial value of the structure's integer field to zero.

This function is designed to be called during the DSM segment initialization phase to ensure the shared memory structure is properly initialized before being used by multiple processes.

## Parameters / Member Variables
- `ptr`: Raw pointer to the allocated shared memory that will be cast to `TestDSMRegistryStruct`

## Dependencies
- Functions called/Symbols referenced:
  - `LWLockInitialize` - Initializes the lightweight lock
  - `LWLockNewTrancheId` - Allocates a new lock tranche ID
  - `TestDSMRegistryStruct` - The structure type being initialized
- Called from (representative examples):
  - `tdr_attach_shmem` - During DSM segment attachment process

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Part of the test_dsm_registry test module, used for testing PostgreSQL's DSM functionality
- The function assumes the provided pointer has sufficient space for a `TestDSMRegistryStruct`
- The lock initialization uses a new tranche ID, which helps with lock contention monitoring and debugging