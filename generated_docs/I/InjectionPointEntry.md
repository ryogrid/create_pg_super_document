# InjectionPointEntry

## Location
[src/backend/utils/misc/injection_point.c:40-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/injection_point.c#L40-L71)

## Overview
InjectionPointEntry is a structure that represents a single injection point stored in shared memory, designed for lock-free access using a generation counter protocol.

## Definition

```c
typedef struct InjectionPointEntry
{
	/*
	 * Because injection points need to be usable without LWLocks, we use a
	 * generation counter on each entry to allow safe, lock-free reading.
	 *
	 * To read an entry, first read the current 'generation' value.  If it's
	 * even, then the slot is currently unused, and odd means it's in use.
	 * When reading the other fields, beware that they may change while
	 * reading them, if the entry is released and reused!  After reading the
	 * other fields, read 'generation' again: if its value hasn't changed, you
	 * can be certain that the other fields you read are valid.  Otherwise,
	 * the slot was concurrently recycled, and you should ignore it.
	 *
	 * When adding an entry, you must store all the other fields first, and
	 * then update the generation number, with an appropriate memory barrier
	 * in between. In addition to that protocol, you must also hold
	 * InjectionPointLock, to prevent two backends from modifying the array at
	 * the same time.
	 */
	pg_atomic_uint64 generation;

	char		name[INJ_NAME_MAXLEN];	/* point name */
	char		library[INJ_LIB_MAXLEN];	/* library */
	char		function[INJ_FUNC_MAXLEN];	/* function */

	/*
	 * Opaque data area that modules can use to pass some custom data to
	 * callbacks, registered when attached.
	 */
	char		private_data[INJ_PRIVATE_MAXLEN];
} InjectionPointEntry;
```
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
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md)
  - INJ_NAME_MAXLEN (64)
  - INJ_LIB_MAXLEN (128)
  - INJ_FUNC_MAXLEN (128)
  - INJ_PRIVATE_MAXLEN (1024)
- Called from (representative examples):
  - [InjectionPointsCtl](InjectionPointsCtl.md)
  - [injection_point_cache_load](../i/injection_point_cache_load.md)
  - [InjectionPointAttach](InjectionPointAttach.md)
  - [InjectionPointDetach](InjectionPointDetach.md)
  - [InjectionPointCacheRefresh](InjectionPointCacheRefresh.md)

## Notes and Other Information
The structure implements a lock-free reading protocol that requires careful attention to memory ordering. Readers must:
1. Read the generation counter
2. Read other fields
3. Re-read the generation counter to verify consistency

Writers must hold InjectionPointLock and update all fields before incrementing the generation counter with appropriate memory barriers. This design allows the injection point system to be used in performance-critical code paths without the overhead of traditional locking mechanisms.