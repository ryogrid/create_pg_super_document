# InjectionPointEntry

## Location
src/backend/utils/misc/injection_point.c: 40 - 71

## Overview
InjectionPointEntry is a structure that represents a single injection point stored in shared memory, designed for lock-free access using a generation counter protocol.

## Definition


## Detailed Description
InjectionPointEntry represents a single injection point in PostgreSQL's injection point system, which allows for runtime code injection for testing and debugging purposes. The structure is specifically designed to be stored in shared memory and accessed without LWLocks using a sophisticated generation counter protocol.

The key design feature is the lock-free access pattern: readers check the generation counter before and after reading other fields to ensure consistency. An even generation value indicates the slot is unused, while an odd value indicates it's in use. Writers must hold InjectionPointLock and follow a specific protocol when updating entries.

## Parameters / Member Variables
- : Atomic 64-bit generation counter used for lock-free access protocol. Even values indicate unused slots, odd values indicate active entries.
- : Name of the injection point (maximum 64 characters including null terminator)
- : Name of the library containing the injection point callback (maximum 128 characters)
- : Name of the callback function to be invoked (maximum 128 characters)
- : Opaque data area for passing custom data to callbacks (maximum 1024 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint64
  - INJ_NAME_MAXLEN (64)
  - INJ_LIB_MAXLEN (128)
  - INJ_FUNC_MAXLEN (128)
  - INJ_PRIVATE_MAXLEN (1024)
- Called from (representative examples):
  - InjectionPointsCtl
  - injection_point_cache_load
  - InjectionPointAttach
  - InjectionPointDetach
  - InjectionPointCacheRefresh

## Notes and Other Information
The structure implements a lock-free reading protocol that requires careful attention to memory ordering. Readers must:
1. Read the generation counter
2. Read other fields
3. Re-read the generation counter to verify consistency

Writers must hold InjectionPointLock and update all fields before incrementing the generation counter with appropriate memory barriers. This design allows the injection point system to be used in performance-critical code paths without the overhead of traditional locking mechanisms.