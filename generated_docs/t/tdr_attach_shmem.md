# tdr_attach_shmem

## Location
src/test/modules/test_dsm_registry/test_dsm_registry.c: 39 - 51

## Overview
Attaches to a named dynamic shared memory segment for the test_dsm_registry module and registers the associated lightweight lock tranche.

## Definition
```c
static void tdr_attach_shmem(void)
```

## Detailed Description
The `tdr_attach_shmem` function is responsible for obtaining access to a named DSM (Dynamic Shared Memory) segment called "test_dsm_registry". It uses PostgreSQL's DSM registry system to either attach to an existing segment or create a new one if it doesn't exist. The function calls `GetNamedDSMSegment` with the initialization callback `tdr_init_shmem` to ensure proper initialization of the shared memory structure. After obtaining the segment, it registers the lightweight lock tranche with a descriptive name for monitoring and debugging purposes.

The function sets the global `tdr_state` pointer to reference the shared memory structure, making it available for subsequent operations within the test module.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - `GetNamedDSMSegment` - Obtains or creates the named DSM segment
  - `LWLockRegisterTranche` - Registers the lock tranche with the system
  - `tdr_init_shmem` - Initialization callback for new segments
  - `TestDSMRegistryStruct` - The structure type for the shared memory segment
- Called from (representative examples):
  - `set_val_in_shmem` - Before setting values in shared memory
  - `get_val_in_shmem` - Before reading values from shared memory

## Notes and Other Information
- This is a static function, accessible only within the same compilation unit
- Part of the test_dsm_registry test module for testing PostgreSQL's DSM functionality
- The function updates the global `tdr_state` variable to point to the shared memory segment
- The `found` variable indicates whether the segment already existed, though it's not used in this implementation
- Lock tranche registration helps with lock contention monitoring and provides meaningful names in debugging tools