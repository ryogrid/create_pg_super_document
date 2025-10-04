# test_create

## Location
[src/test/modules/test_tidstore/test_tidstore.c:86-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_tidstore/test_tidstore.c#L86-L135)

## Overview
A PostgreSQL C function that creates and initializes a TidStore for testing purposes, with support for both local and shared memory configurations.

## Definition

```c
Datum
test_create(PG_FUNCTION_ARGS)
```
## Detailed Description
This function creates a TidStore (Tuple Identifier Store) for testing the tidstore functionality. It supports two creation modes:
- **Shared mode**: Creates the tidstore using Dynamic Shared Area (DSA) for inter-process communication
- **Local mode**: Creates the tidstore in local memory context

The function initializes several key components:
1. Creates the TidStore with a maximum size hint of 2MB
2. Sets up arrays for storing test ItemPointers (insert_tids, lookup_tids, iter_tids) with initial capacity of 256 items each
3. Records the empty tidstore size for later memory usage calculations
4. For shared tidstores, registers a new LWLock tranche and pins the DSA mapping

The tidstore is created in TopMemoryContext to persist across multiple test function calls within the same backend process.

## Parameters / Member Variables
- Function takes one boolean argument via PG_GETARG_BOOL(0):
  - : If true, creates a shared tidstore using DSA; if false, creates a local tidstore

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - [LWLockNewTrancheId](../L/LWLockNewTrancheId.md)
  - [LWLockRegisterTranche](../L/LWLockRegisterTranche.md)
  - [TidStoreCreateShared](../T/TidStoreCreateShared.md)
  - [TidStoreGetDSA](../T/TidStoreGetDSA.md)
  - [dsa_pin_mapping](../d/dsa_pin_mapping.md)
  - [TidStoreCreateLocal](../T/TidStoreCreateLocal.md)
  - [TidStoreMemoryUsage](../T/TidStoreMemoryUsage.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - This is a SQL-callable function, typically invoked from test scripts

## Notes and Other Information
- This is a PostgreSQL extension function exposed to SQL for testing purposes
- The function uses global variables (tidstore, items) to maintain state across test calls
- The shared tidstore uses DSA but is still only usable by the same process that created it
- The function includes memory context switching to ensure proper memory management
- Uses VACUUM's opposite configuration (insert_exact=false) for broader test coverage
- The function will assert if called when tidstore is already initialized

## Simplified Source

```c
Datum
test_create(PG_FUNCTION_ARGS)
{
    bool shared = PG_GETARG_BOOL(0);
    size_t tidstore_max_size = 2 * 1024 * 1024;  // 2MB hint
    size_t array_init_size = 1024;

    Assert(tidstore == NULL);

    // Switch to TopMemoryContext for persistence across tests
    MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);

    if (shared) {
        // Create shared tidstore with DSA
        int tranche_id = LWLockNewTrancheId();
        LWLockRegisterTranche(tranche_id, "test_tidstore");
        tidstore = TidStoreCreateShared(tidstore_max_size, tranche_id);
        dsa_pin_mapping(TidStoreGetDSA(tidstore));  // Keep DSA pinned
    } else {
        // Create local tidstore (insert_exact=false for broader testing)
        tidstore = TidStoreCreateLocal(tidstore_max_size, false);
    }

    // Initialize test arrays and record empty size
    tidstore_empty_size = TidStoreMemoryUsage(tidstore);
    items.num_tids = 0;
    items.max_tids = array_init_size / sizeof(ItemPointerData);
    items.insert_tids = palloc0(array_init_size);
    items.lookup_tids = palloc0(array_init_size);
    items.iter_tids = palloc0(array_init_size);

    MemoryContextSwitchTo(old_ctx);
    PG_RETURN_VOID();
}
```