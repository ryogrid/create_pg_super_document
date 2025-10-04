# assign_backendlist_entry

## Location
[src/backend/postmaster/postmaster.c:4347-4410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4347-L4410)

## Overview
Allocates and initializes a Backend structure for a background worker without adding it to the active backend list, performing necessary resource checks and setup.

## Definition
```c
static bool assign_backendlist_entry(RegisteredBgWorker *rw)
```

## Detailed Description
This function handles the resource allocation and initialization phase for starting a background worker. It performs several critical setup steps:

1. **Connection Limit Check**: Verifies database can accept another connection using canAcceptConnections()
2. **Cancel Key Generation**: Creates a random cancel key for the worker session for security
3. **Backend Structure Allocation**: Allocates memory for the Backend structure with no-OOM handling
4. **Backend Initialization**: Sets up backend type, child slot assignment, and initial state
5. **RegisteredBgWorker Update**: Links the allocated Backend to the worker registration

The function is designed to handle failures gracefully without changing worker state, allowing calling code to treat failures as crashes and implement appropriate retry logic.

## Parameters / Member Variables
- `rw`: Pointer to RegisteredBgWorker structure that will be linked to the allocated Backend

## Dependencies
- Functions called/Symbols referenced:
  - [canAcceptConnections](../c/canAcceptConnections.md)
  - [RandomCancelKey](../R/RandomCancelKey.md)
  - [palloc_extended](../p/palloc_extended.md)
  - [AssignPostmasterChildSlot](../A/AssignPostmasterChildSlot.md)
  - BACKEND_TYPE_BGWORKER
  - CAC_OK
  - MCXT_ALLOC_NO_OOM
- Called from (representative examples):
  - [do_start_bgworker](../d/do_start_bgworker.md)
  - SignalChildren

## Notes and Other Information
- Returns true on success, false on any failure
- Does not add Backend to active lists - that's done by the caller after successful fork
- Uses no-OOM allocation to avoid elog() calls that could interfere with fork operations
- Assigns random cancel keys even though background workers may not need them for security
- Child slot assignment is critical for process tracking and cleanup
- Failure cases include connection limits, cancel key generation failure, and out-of-memory conditions

## Simplified Source

```c
static bool assign_backendlist_entry(RegisteredBgWorker *rw) {
    Backend *bn;

    // Check if database can accept another connection
    if (canAcceptConnections(BACKEND_TYPE_BGWORKER) != CAC_OK) {
        ereport(LOG, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                     errmsg("no slot available for new background worker process")));
        return false;
    }

    // Generate random cancel key for security
    if (!RandomCancelKey(&MyCancelKey)) {
        ereport(LOG, (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("could not generate random cancel key")));
        return false;
    }

    // Allocate Backend structure with no-OOM handling
    bn = palloc_extended(sizeof(Backend), MCXT_ALLOC_NO_OOM);
    if (bn == NULL) {
        ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY),
                     errmsg("out of memory")));
        return false;
    }

    // Initialize Backend structure
    bn->cancel_key = MyCancelKey;
    bn->child_slot = MyPMChildSlot = AssignPostmasterChildSlot();
    bn->bkend_type = BACKEND_TYPE_BGWORKER;
    bn->dead_end = false;
    bn->bgworker_notify = false;

    // Link Backend to RegisteredBgWorker
    rw->rw_backend = bn;
    rw->rw_child_slot = bn->child_slot;

    return true;
}
```